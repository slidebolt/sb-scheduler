package main

import (
	"github.com/slidebolt/sb-runtime"
	"github.com/slidebolt/sb-scheduler/app"
)

func main() { runtime.Run(app.New()) }
