//go:debug x509negativeserial=1

package sqlserver

import (
	"fmt"
	"testing"

	"github.com/goravel/framework/mocks/config"
	"github.com/goravel/sqlserver/contracts"
	"github.com/stretchr/testify/suite"
)

type DockerTestSuite struct {
	suite.Suite
	connection string
	database   string
	username   string
	password   string

	mockConfig *config.Config
	docker     *Docker
}

func TestDockerTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(DockerTestSuite))
}

func (s *DockerTestSuite) SetupTest() {
	s.connection = "default"
	s.database = "goravel"
	s.username = "goravel"
	s.password = "Framework!123"
	s.mockConfig = config.NewConfig(s.T())
	s.docker = NewDocker(NewConfig(s.mockConfig, s.connection), s.database, s.username, s.password)
}

func (s *DockerTestSuite) Test_Build_Config_AddData_Fresh_Shutdown() {
	s.Nil(s.docker.Build())

	instance, err := s.docker.connect()
	s.Nil(err)
	s.NotNil(instance)

	s.Equal("127.0.0.1", s.docker.Config().Host)
	s.Equal(s.database, s.docker.Config().Database)
	s.Equal(s.username, s.docker.Config().Username)
	s.Equal(s.password, s.docker.Config().Password)
	s.True(s.docker.Config().Port > 0)

	res := instance.Exec(`
CREATE TABLE users (
	id bigint NOT NULL IDENTITY(1,1),
	name varchar(255) NOT NULL,
	PRIMARY KEY (id)
);
`)
	s.Nil(res.Error)

	res = instance.Exec(`
INSERT INTO users (name) VALUES ('goravel');
`)
	s.Nil(res.Error)
	s.Equal(int64(1), res.RowsAffected)

	var count int64
	res = instance.Raw("SELECT count(*) FROM sys.tables WHERE name = 'users';").Scan(&count)
	s.Nil(res.Error)
	s.Equal(int64(1), count)

	s.Nil(s.docker.Fresh())

	res = instance.Raw("SELECT count(*) FROM sys.tables WHERE name = 'users';").Scan(&count)
	s.Nil(res.Error)
	s.Equal(int64(0), count)

	databaseDriver, err := s.docker.Database("another")
	s.NoError(err)
	s.NotNil(databaseDriver)

	s.Nil(s.docker.Shutdown())
}

func (s *DockerTestSuite) TestDatabase() {
	s.Nil(s.docker.Build())

	_, err := s.docker.connect()
	s.Nil(err)

	docker, err := s.docker.Database("another")
	s.Nil(err)
	s.NotNil(docker)

	dockerImpl := docker.(*Docker)
	_, err = dockerImpl.connect()
	s.Nil(err)

	s.Nil(s.docker.Shutdown())
}

func (s *DockerTestSuite) TestReady() {
	s.Run("config contains write config", func() {
		s.SetupTest()
		s.Nil(s.docker.Build())

		s.mockConfig.EXPECT().Get(fmt.Sprintf("database.connections.%s.write", s.connection)).Return([]contracts.Config{
			{
				Host: "127.0.0.1",
			},
		}).Once()
		s.mockConfig.EXPECT().Add(fmt.Sprintf("database.connections.%s.write", s.connection), []contracts.Config{
			{
				Host: "127.0.0.1",
				Port: s.docker.databaseConfig.Port,
			},
		}).Once()

		s.Nil(s.docker.Ready())
		s.Nil(s.docker.Shutdown())
	})

	s.Run("config does not contain write config", func() {
		s.SetupTest()
		s.Nil(s.docker.Build())

		s.mockConfig.EXPECT().Get(fmt.Sprintf("database.connections.%s.write", s.connection)).Return(nil).Once()
		s.mockConfig.EXPECT().Add(fmt.Sprintf("database.connections.%s.port", s.connection), s.docker.databaseConfig.Port).Once()

		s.Nil(s.docker.Ready())
		s.Nil(s.docker.Shutdown())
	})
}
