// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"sync"

	"github.com/CodisLabs/codis/pkg/proxy/redis"
	"github.com/CodisLabs/codis/pkg/utils/atomic2"
)

type Dispatcher interface {
	Dispatch(r *Request) error
}

type Request struct {
	OpStr string
	Start int64

	Resp *redis.Resp // 请求数据

	Coalesce func() error // 聚合函数
	Response struct {     // 响应数据
		Resp *redis.Resp
		Err  error
	}

	Wait *sync.WaitGroup
	slot *sync.WaitGroup

	Failed *atomic2.Bool
}
