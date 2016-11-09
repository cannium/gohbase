// Copyright (C) 2016  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package mock

// To run this command, gomock and mockgen need to be installed, by running
//    go get github.com/golang/mock/gomock
//    go get github.com/golang/mock/mockgen
// then run 'go generate' to auto-generate mock_client.

//go:generate mockgen -destination=client.go -source=../../client.go -package=mock
//go:generate mockgen -destination=readerwritercloser.go -package=mock io ReadWriteCloser
//go:generate mockgen -destination=call.go -package=mock github.com/cannium/gohbase/hrpc Call
