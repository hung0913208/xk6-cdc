package cdc

import (
	"encoding/json"
	"errors"

	"github.com/dop251/goja"
	"github.com/hung0913208/xk6-cdc/lib/kafka"
	"go.k6.io/k6/js/common"
)

type readConfig struct {
	Servers []string `json:"servers"`
	Topics  []string `json:"topics"`
	Timeout int      `json:"timeout"`
	Debug   bool     `json:"debug"`
}

type consumeArgs struct {
}

func (self *cdcModuleImpl) readerClass(call goja.ConstructorCall) *goja.Object {
	runtime := self.virtualUser.Runtime()
	config := &readConfig{}

	if len(call.Arguments) == 0 {
		common.Throw(runtime, errors.New("Not enough parameters"))
	}

	if params, ok := call.Argument(0).Export().(map[string]interface{}); ok {
		if b, err := json.Marshal(params); err != nil {
			common.Throw(runtime, err)
		} else if err = json.Unmarshal(b, &config); err != nil {
			common.Throw(runtime, err)
		}
	}

	object := runtime.NewObject()
	reader := kafka.NewKafka()

	err := object.Set("This", reader)
	if err != nil {
		common.Throw(runtime, err)
	}

	err = object.Set("consume", func(call goja.FunctionCall) goja.Value {
		consumeParams := &consumeArgs{}

		if len(call.Arguments) == 0 {
			common.Throw(runtime, errors.New("Need provide arguments"))
		}

		params, ok := call.Argument(0).Export().(map[string]interface{})
		if ok {
			if b, err := json.Marshal(params); err != nil {
				common.Throw(runtime, err)
			} else if err = json.Unmarshal(b, &consumeParams); err != nil {
				common.Throw(runtime, err)
			}
		}

		err := reader.Open(config.Servers,
			config.Topics,
			config.Timeout,
			config.Debug,
		)
		if err != nil {
			common.Throw(runtime, err)
		}

		controller, err := reader.Consume()
		if err != nil {
			common.Throw(runtime, err)
		}

		return runtime.ToValue(controller)
	})
	if err != nil {
		common.Throw(runtime, err)
	}

	err = object.Set("close", func(call goja.FunctionCall) goja.Value {
		if err := reader.Close(); err != nil {
			common.Throw(runtime, err)
		}

		return goja.Undefined()
	})
	if err != nil {
		common.Throw(runtime, err)
	}

	return runtime.ToValue(object).ToObject(runtime)
}
