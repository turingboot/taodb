package skiplist

import (
	"fmt"
	"testing"
)

func Test_slRandomLevel(t *testing.T) {

	for i := 0; i < 100; i++ {
		test := slRandomLevel()
		fmt.Print(test)
	}

}

func TestSkipList_SlCreate(t *testing.T) {
	test := SlCreate()
	if test == nil {
		t.Errorf("test is nil")
	}
}

func TestSkipList_SlInsert(t *testing.T) {
	test := SlCreate()
	if test == nil {
		t.Errorf("test is nil")
	}
	test.SlInsert("ac", 123)
}

func TestSkipList_SlGet(t *testing.T) {
	test := SlCreate()
	if test == nil {
		t.Errorf("test is nil")
	}
	test.SlInsert("ac", 123)

	node, err := test.SlGet("ac")
	if err != nil {
		t.Errorf(err.Error())
	}
	fmt.Println(node.Value())
}

func TestSkipList_SlDelete(t *testing.T) {
	test := SlCreate()
	if test == nil {
		t.Errorf("test is nil")
	}
	test.SlInsert("ac", 123)

	node, err := test.SlGet("ac")
	if err != nil {
		t.Errorf(err.Error())
	}
	fmt.Println(node.Value())
	test.SlDelete("ac")
	node, err = test.SlGet("ac")
	if err != nil {
		t.Log(err.Error())
	}
}
