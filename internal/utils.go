package internal

import "reflect"

// Deep copy
func Copy(source interface{}, destin interface{}) {
	x := reflect.ValueOf(source)
	dest := reflect.ValueOf(destin)
	if dest.Kind() != reflect.Ptr {
		panic("destin must be a pointer")
	}

	if x.Kind() == reflect.Ptr {
		starX := x.Elem()
		y := reflect.New(starX.Type())
		starY := y.Elem()
		starY.Set(starX)
		dest.Elem().Set(y.Elem())
	} else {
		dest.Elem().Set(x)
	}
}
