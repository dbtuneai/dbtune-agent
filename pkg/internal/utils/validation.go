package utils

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/go-playground/validator/v10"
)

// ValidateStruct validates a struct using the validator package
// It returns a single error with all validation errors combined
// Used to validate configs when we start the agent
func ValidateStruct(s interface{}) error {
	// Check for nil input
	if s == nil {
		return fmt.Errorf("invalid validation: input is nil")
	}

	validate := validator.New()

	err := validate.Struct(s)
	if err != nil {
		if _, ok := err.(*validator.InvalidValidationError); ok {
			return fmt.Errorf("invalid validation: %v", err)
		}

		var errMsgs []string
		for _, err := range err.(validator.ValidationErrors) {
			fieldName := err.Field()

			// Try to get the mapstructure tag to use as field name
			field, _ := reflect.TypeOf(s).Elem().FieldByName(fieldName)
			if mapstructureTag := field.Tag.Get("mapstructure"); mapstructureTag != "" {
				fieldName = mapstructureTag
			}

			errMsgs = append(errMsgs, fmt.Sprintf("%s is required or invalid. %v", fieldName, err.Error()))
		}

		return errors.New(strings.Join(errMsgs, ", "))
	}

	return nil
}
