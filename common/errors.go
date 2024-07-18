package common

import (
	"fmt"

	"github.com/go-playground/validator/v10"
	"github.com/iancoleman/strcase"
)

// Error is a domain error that includes a code, message, meta
type Error struct {
	Code     string                 `json:"code,omitempty"`
	Message  string                 `json:"message,omitempty"`
	Internal error                  `json:"internal,omitempty"`
	Meta     map[string]interface{} `json:"meta,omitempty"`
}

// NewError creates a new domain error
func NewError(code, message string) *Error {
	return &Error{
		Code:    code,
		Message: message,
	}
}

// IncludeMeta includes meta data in the error
func (e *Error) IncludeMeta(meta map[string]interface{}) *Error {
	return &Error{
		Code:     e.Code,
		Message:  e.Message,
		Internal: e.Internal,
		Meta:     meta,
	}
}

// Error implements the error interface
func (e *Error) Error() string {
	if e.Internal == nil {
		return e.Message
	}

	return fmt.Sprintf("%s: %s", e.Message, e.Internal)
}

// ErrorFromValidationErrors creates a domain error from a list of validation errors
func ErrorFromValidationErrors(errs validator.ValidationErrors) *Error {
	meta := make(map[string]interface{})
	for _, err := range errs {
		meta[strcase.ToSnake(err.Field())] = err.ActualTag()
	}

	return NewError("invalid_request", "validation error").IncludeMeta(meta)
}
