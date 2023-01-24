package cdc

import (
	"github.com/dop251/goja"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
)

func init() {
	modules.Register("k6/x/cdc", new(RootModule))
}

type RootModule struct{}

type cdcModuleImpl struct {
	virtualUser modules.VU
	metrics     *cdcMetrics
	exports     *goja.Object
}

var (
	_ modules.Module   = &RootModule{}
	_ modules.Instance = &cdcModuleImpl{}
)

func (*RootModule) NewModuleInstance(virtualUser modules.VU) modules.Instance {
	runtime := virtualUser.Runtime()

	metrics, err := registerMetrics(virtualUser)
	if err != nil {
		common.Throw(virtualUser.Runtime(), err)
	}

	moduleInstance := &cdcModuleImpl{
		virtualUser: virtualUser,
		metrics:     metrics,
		exports:     runtime.NewObject(),
	}

	mustExport := func(name string, value interface{}) {
		err := moduleInstance.exports.Set(name, value)
		if err != nil {
			common.Throw(runtime, err)
		}
	}

	mustExport("Writer", moduleInstance.writerClass)
	mustExport("Reader", moduleInstance.readerClass)

	return moduleInstance
}

func (self *cdcModuleImpl) Exports() modules.Exports {
	return modules.Exports{
		Default: self.exports,
	}
}
