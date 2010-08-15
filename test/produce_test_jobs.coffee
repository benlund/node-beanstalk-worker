client: require('beanstalk_client').Client
sys: require 'sys'

#producer
client.connect null, (err, conn) ->
  if err
    sys.puts 'Producer connection error:', sys,inspect(err)
  else
		sys.puts('Producer connected')

		conn.use 'test', () ->
      send_job: () ->
        conn.put 0, 0, 60, JSON.stringify({type: 'test', data: {some: ['random', {'data': true}]}}), (err, job_id) ->
          sys.puts('Producer sent job: ' + job_id)
          setTimeout(send_job, 1000)
      send_job()

