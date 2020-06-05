package psr

import (
	"errors"
	"github.com/k0kubun/pp"
	"net/url"
	"psr-cli/pb"
)

type Lookuper struct {
	cli *Client
}

func newLookuper(cli *Client) *Lookuper {
	return &Lookuper{
		cli: cli,
	}
}

func (l *Lookuper) lookup(topic string) (*url.URL, error) {
	t := pb.BaseCommand_LOOKUP
	reqId := l.cli.nextReqId()
	cmd := &pb.BaseCommand{
		Type: &t,
		LookupTopic: &pb.CommandLookupTopic{
			RequestId: &reqId,
			Topic:     &topic,
		},
	}
	resp, err := l.cli.conn.sendCmd(reqId, cmd)
	if err != nil {
		return nil, err
	}
	if resp.LookupTopicResponse == nil {
		return nil, errors.New("empty lookup resp")
	}
	r := resp.LookupTopicResponse
	switch *r.Response {
	case pb.CommandLookupTopicResponse_Redirect:
		pp.Println("lookup redirected...", r.Message) // recursively lookup
	case pb.CommandLookupTopicResponse_Connect:
		return url.ParseRequestURI(r.GetBrokerServiceUrl())
	case pb.CommandLookupTopicResponse_Failed:
		pp.Println("lookup failed...")
	}
	return nil, errors.New("invalid lookup resp")
}
