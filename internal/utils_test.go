package internal_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/violetpay-org/queue-streamer/internal"
)

type TestStruct struct {
	Name string
	Age  int
}

func TestCopy(t *testing.T) {
	t.Run("Copy pointer", func(t *testing.T) {

		source := &TestStruct{Name: "John", Age: 25}
		var destin TestStruct

		internal.Copy(source, &destin)

		assert.Equal(t, source, &destin)
		assert.Equal(t, source.Name, destin.Name)
	})

	t.Run("Copy value", func(t *testing.T) {

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
