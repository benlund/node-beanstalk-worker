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
        job_data: {type: 'http_request', data: {method: 'POST', url: 'http://127.0.0.1:9876/search?q=blah', headers:{}, body: 'bodystring'}}
        conn.put 0, 0, 1, JSON.stringify(job_data), (err, job_id) ->
          sys.puts('Producer sent job: ' + job_id)
          setTimeout(send_job, 1000)
      send_job()

