/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/*
Package etcd2topo implements topo.Server with etcd as the backend.

We expect the following behavior from the etcd client library:

  - Get and Delete return ErrorCodeKeyNotFound if the node doesn't exist.
  - Create returns ErrorCodeNodeExist if the node already exists.
  - Intermediate directories are always created automatically if necessary.
  - Set returns ErrorCodeKeyNotFound if the node doesn't exist already.
  - It returns ErrorCodeTestFailed if the provided version index doesn't match.

We follow these conventions within this package:

  - Call convertError(err) on any errors returned from the etcd client library.
    Functions defined in this package can be assumed to have already converted
    errors as necessary.
*/
package etcd2topo

import (
	"crypto/tls"
	"flag"
	"github.com/coreos/etcd/pkg/transport"
	"io/ioutil"
	"strings"
	"time"
	"vitess.io/vitess/go/vt/log"

	"github.com/coreos/etcd/clientv3"
	"vitess.io/vitess/go/vt/topo"
)

var (
	certPath = flag.String("topo_etcd2_tls_cert", "", "the cert to use to connect to the etcd topo server, requires topo_etcd2_tls_key, enables TLS")
	keyPath = flag.String("topo_etcd2_tls_key", "", "the key to use to connect to the etcd topo server, enables TLS")
	caPath  = flag.String("topo_etcd2_tls_ca", "", "the server ca to use to validate servers when connecting to the etcd topo server")
)

// Factory is the consul topo.Factory implementation.
type Factory struct{}

// HasGlobalReadOnlyCell is part of the topo.Factory interface.
func (f Factory) HasGlobalReadOnlyCell(serverAddr, root string) bool {
	return false
}

// Create is part of the topo.Factory interface.
func (f Factory) Create(cell, serverAddr, root string) (topo.Conn, error) {
	return NewServer(serverAddr, root)
}

// Server is the implementation of topo.Server for etcd.
type Server struct {
	// cli is the v3 client.
	cli *clientv3.Client

	// root is the root path for this client.
	root string
}

// Close implements topo.Server.Close.
// It will nil out the global and cells fields, so any attempt to
// re-use this server will panic.
func (s *Server) Close() {
	s.cli.Close()
	s.cli = nil
}

// TODO: Rename this to NewServer and change NewServer to a name that signifies it's global.
func NewServerWithOpts(serverAddr, root string, certPath *string, keyPath *string, caPath *string) (*Server, error){
	config := clientv3.Config{
		Endpoints:  strings.Split(serverAddr, ","),
		DialTimeout: 5 * time.Second,
	}

	// If TLS is enabled, attach TLS config info.
	if *certPath != "" && *keyPath != "" {
		// Verify we can load provided cert and key.
		_, err := tls.LoadX509KeyPair(*certPath, *keyPath)
		if err != nil {
			log.Fatalf("Unable to load cert %v and key %v, err %v", *certPath, *keyPath, err)
		}

		// Verify we can open ca cert.
		_, err = ioutil.ReadFile(*caPath)
		if err != nil {
			log.Fatalf("Unable to open ca cert %v, err %v", *caPath, err)
		}

		// Safe now to build up TLS info.
		tlsInfo := transport.TLSInfo{
			CertFile:   *certPath,
			KeyFile:    *keyPath,
			TrustedCAFile: *caPath,
		}

		tlsConfig, err := tlsInfo.ClientConfig()
		if err != nil {
			log.Fatalf("Unable to generate client config, err %v", err)
		}
		config.TLS = tlsConfig
	}

	cli, err := clientv3.New(config)
	if err != nil {
		return nil, err
	}

	return &Server{
		cli:  cli,
		root: root,
	}, nil
}

// TODO: Rename this to a name to signifies this is a global etcd server.
// NewServer returns a new etcdtopo.Server.
func NewServer(serverAddr, root string) (*Server, error) {
	return NewServerWithOpts(serverAddr, root, certPath, keyPath, caPath)
}

func init() {
	topo.RegisterFactory("etcd2", Factory{})
}
