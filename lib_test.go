package s3fileprocessor

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestFoo(t *testing.T) {
	c := qt.New(t)
	c.Assert(Foo(), qt.Equals, "foo")
}
