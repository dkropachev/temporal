package gocql

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/gocql/gocql"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
)

func ConvertError(
	operation string,
	err error,
) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, gocql.ErrTimeoutNoResponse) || errors.Is(err, gocql.ErrConnectionClosed) {
		return &persistence.TimeoutError{Msg: fmt.Sprintf("operation %v encountered %v", operation, err.Error())}
	}
	if errors.Is(err, gocql.ErrNotFound) {
		return serviceerror.NewNotFoundf("operation %v encountered %v", operation, err.Error())
	}

	var cqlTimeoutErr *gocql.RequestErrWriteTimeout
	if errors.As(err, &cqlTimeoutErr) {
		return &persistence.TimeoutError{Msg: fmt.Sprintf("operation %v encountered %v", operation, cqlTimeoutErr.Error())}
	}

	var cqlRequestErr gocql.RequestError
	if errors.As(err, &cqlRequestErr) {
		if cqlRequestErr.Code() == gocql.ErrCodeOverloaded {
			return &serviceerror.ResourceExhausted{
				Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED,
				Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
				Message: fmt.Sprintf("operation %v encountered %v", operation, cqlRequestErr.Error()),
			}
		}

		if cqlRequestErr.Code() == gocql.ErrCodeInvalid {
			// NB: See https://cassandra.apache.org/_/blog/Apache-Cassandra-4.1-Features-Guardrails-Framework.html
			if strings.Contains(strings.ToLower(cqlRequestErr.Message()), "disk usage exceeds failure threshold") {
				return &serviceerror.ResourceExhausted{
					Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_STORAGE_LIMIT,
					Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
					Message: fmt.Sprintf("operation %v encountered %v", operation, cqlRequestErr.Error()),
				}
			}

			return serviceerror.NewUnavailablef("operation %v encountered %v", operation, cqlRequestErr.Error())
		}
	}

	if e, ok := errors.AsType[*serviceerror.ResourceExhausted](err); ok {
		return e
	}

	return serviceerror.NewUnavailablef("operation %v encountered %v", operation, err.Error())
}

func IsNotFoundError(err error) bool {
	return errors.Is(err, gocql.ErrNotFound)
}

func IsUnconfiguredTableError(err error, table string) bool {
	if err == nil {
		return false
	}
	code, message, ok := cassandraRequestError(err)
	if !ok || (code != gocql.ErrCodeInvalid && code != gocql.ErrCodeConfig) {
		return false
	}
	message = strings.ToLower(message)
	if !strings.Contains(message, "unconfigured table") {
		return false
	}
	table = strings.ToLower(table)
	for _, field := range strings.Fields(message) {
		field = strings.NewReplacer(`"`, "", "'", "", "`", "").Replace(field)
		field = strings.Trim(field, ";,.")
		if field == table || strings.HasSuffix(field, "."+table) {
			return true
		}
	}
	return false
}

func cassandraRequestError(err error) (int, string, bool) {
	var requestErr gocql.RequestError
	if errors.As(err, &requestErr) {
		return requestErr.Code(), requestErr.Message(), true
	}

	// The Scylla gocql fork exposes protocol errors through getter methods.
	var getterError interface {
		error
		GetCode() int
		GetMessage() string
	}
	if errors.As(err, &getterError) {
		return getterError.GetCode(), getterError.GetMessage(), true
	}
	return 0, "", false
}
