package publish

import (
	"github.com/lightwebinc/bcommon/nodeapi"

	"github.com/lightwebinc/bstore/chaincfg"
)

// nodeView is the chain view and tip of a test chain's node at url.
func nodeView(url string) *chaincfg.View {
	v, err := chaincfg.Open(chaincfg.Spec{Network: "regtest", Chain: "asset:" + url, Settle: "rpc:" + url + "/rpc", Headers: url})
	if err != nil {
		panic(err)
	}
	return v
}

func nodeChain(url string) nodeapi.Chain { return nodeView(url).Chain }
func nodeTip(url string) chaincfg.Tip    { return nodeView(url).Tip }
