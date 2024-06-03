package internal_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/violetpay-org/queue-streamer/internal"
	"testing"
)

// Deep copy
//
//	func Copy(source interface{}, destin interface{}) {
//		x := reflect.ValueOf(source)
//		if x.Kind() == reflect.Ptr {
//			starX := x.Elem()
//			y := reflect.New(starX.Type())
//			starY := y.Elem()
//			starY.Set(starX)
//			reflect.ValueOf(destin).Elem().Set(y.Elem())
//		} else {
//			destin = x.Interface()
//		}
//	}

func TestCopy(t *testing.T) {
	t.Parallel()

	t.Run("Copy pointer", func(t *testing.T) {
		type TestStruct struct {
			Name string
			Age  int
		}

		source := &TestStruct{Name: "John", Age: 25}
		var destin TestStruct

		internal.Copy(source, &destin)

		assert.Equal(t, source, &destin)
		assert.Equal(t, source.Name, destin.Name)
	})

	t.Run("Copy value", func(t *testing.T) {
		type TestStruct struct {
			Name string
			Age  int
		}

		source := TestStruct{Name: "John", Age: 25}
		var destin TestStruct

		internal.Copy(source, &destin)

		assert.Equal(t, source, destin)
		assert.Equal(t, source.Name, destin.Name)
	})

	t.Run("Destination not pointer", func(t *testing.T) {
		type TestStruct struct {
			Name string
			Age  int
		}

		source := TestStruct{Name: "John", Age: 25}
		var destin TestStruct

		assert.Panics(t, func() {
			internal.Copy(source, destin)
		})
	})
}
