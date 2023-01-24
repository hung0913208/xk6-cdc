package cdc

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dop251/goja"
	"github.com/hung0913208/xk6-cdc/lib/kafka"
	"github.com/hung0913208/xk6-cdc/lib/pg"
	"go.k6.io/k6/js/common"
)

type pgConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Dbname   string `json:"dbname"`
	User     string `json:"user"`
	Password string `json:"password"`
}

type kafkaConfig struct {
	Servers []string `json:"servers"`
	Timeout int      `json:"timeout"`
	Debug   bool     `json:"debug"`
}

type writeConfig struct {
	Pg    pgConfig    `json:"pg"`
	Kafka kafkaConfig `json:"kafka"`
}

type writerObject struct {
	pgobj pg.Pg
	kfobj kafka.Kafka
}

type insertArgs struct {
	Table  string     `json:"table"`
	Fields [][]string `json:"fields"`
}

type createArgs struct {
	Table       string   `json:"table"`
	Description []string `json:"description"`
}

type publishArgs struct {
	Servers []string `json:"servers"`
	Topic   string   `json:"topic"`
	Value   string   `json:"value"`
}

func (self *cdcModuleImpl) writerClass(call goja.ConstructorCall) *goja.Object {
	runtime := self.virtualUser.Runtime()
	config := &writeConfig{}

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
	writer := &writerObject{
		pgobj: pg.NewPg(),
		kfobj: kafka.NewKafka(),
	}

	err := object.Set("This", writer)
	if err != nil {
		common.Throw(runtime, err)
	}

	err = object.Set("publish", func(call goja.FunctionCall) goja.Value {
		publishParams := &publishArgs{}

		if params, ok := call.Argument(0).Export().(map[string]interface{}); ok {
			if b, err := json.Marshal(params); err != nil {
				common.Throw(runtime, err)
			} else if err = json.Unmarshal(b, &publishParams); err != nil {
				common.Throw(runtime, err)
			}
		}

		err := writer.kfobj.Open(
			publishParams.Servers,
			nil,
			config.Kafka.Timeout,
			config.Kafka.Debug,
		)
		if err != nil {
			common.Throw(runtime, err)
		}

		msg, err := writer.kfobj.Produce(
			publishParams.Topic,
			publishParams.Value,
		)
		if err != nil {
			common.Throw(runtime, err)
		}
		return runtime.ToValue(msg)
	})
	if err != nil {
		common.Throw(runtime, err)
	}

	err = object.Set("create", func(call goja.FunctionCall) goja.Value {
		createParams := &createArgs{}

		if params, ok := call.Argument(0).Export().(map[string]interface{}); ok {
			if b, err := json.Marshal(params); err != nil {
				common.Throw(runtime, err)
			} else if err = json.Unmarshal(b, &createParams); err != nil {
				common.Throw(runtime, err)
			}
		}

		return runtime.ToValue(
			writer.pgobj.Create(
				createParams.Table,
				createParams.Description,
			),
		)
	})
	if err != nil {
		common.Throw(runtime, err)
	}

	err = object.Set("insert", func(call goja.FunctionCall) goja.Value {
		insertParams := &insertArgs{}

		if params, ok := call.Argument(0).Export().(map[string]interface{}); ok {
			if b, err := json.Marshal(params); err != nil {
				common.Throw(runtime, err)
			} else if err = json.Unmarshal(b, &insertParams); err != nil {
				common.Throw(runtime, err)
			}
		}

		err := writer.pgobj.Open(fmt.Sprintf(
			"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			config.Pg.Host,
			config.Pg.Port,
			config.Pg.User,
			config.Pg.Password,
			config.Pg.Dbname,
		))
		if err != nil {
			common.Throw(runtime, err)
		}

		return runtime.ToValue(
			writer.pgobj.Insert(
				insertParams.Table,
				insertParams.Fields,
			),
		)
	})
	if err != nil {
		common.Throw(runtime, err)
	}

	err = object.Set("close", func(call goja.FunctionCall) goja.Value {
		if err := writer.pgobj.Close(); err != nil {
			common.Throw(runtime, err)
		}
		if err := writer.kfobj.Close(); err != nil {
			common.Throw(runtime, err)
		}
		return goja.Undefined()
	})
	if err != nil {
		common.Throw(runtime, err)
	}

	return runtime.ToValue(object).ToObject(runtime)
}
