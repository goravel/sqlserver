package sqlserver

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type WrapTestSuite struct {
	suite.Suite
	wrap *Wrap
}

func TestWrapSuite(t *testing.T) {
	suite.Run(t, new(WrapTestSuite))
}

func (s *WrapTestSuite) SetupTest() {
	s.wrap = NewWrap("prefix_")
}

func (s *WrapTestSuite) TestQuotes() {
	result := s.wrap.Quotes([]string{"value1", "value2"})
	s.Equal([]string{"N'value1'", "N'value2'"}, result)
}
