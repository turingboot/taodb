package skiplist

import (
	"testing"
)

func Test_slCreateNode(t *testing.T) {

	test := slCreateNode(1, "test", nil)
	if test == nil {
		t.Errorf("test is nil")
	}

}
