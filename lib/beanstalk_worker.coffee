events: require 'events'
client: require('beanstalk_client').Client

class BeanstalkWorker extends events.EventEmitter

  constructor: (id, server, handlers, logger) ->
    @id: id
    @server: server
    @handlers: handlers
    @logger: logger || console

    @stopped: false


  start: (tubes, ignore_default) ->
    @log 'Starting...'

    @on 'next', () =>
      @handle_next()

    client.connect @server, (err, conn) =>
      if err
        @log 'Error connecting: ' + err
      else
        @connection: conn

        @watch_tubes tubes, () =>
          ignored: []
          if ignore_default
          	ignored.push 'default'

          @ignore_tubes ignored, () =>
          	@log 'Started'
          	@emit 'next'


  watch_tubes: (tubes, done) ->
    if tubes && (tube: tubes[0])
      @log 'Watching tube ' + tube
      @connection.watch tube, (err) =>
        if err
          @log 'Error watching tube : ' + tube
        @watch_tubes tubes.slice(1), done

    else
      done()


  ignore_tubes: (tubes, done) ->
    if tubes && (tube: tubes[0])
      @log 'Ignoring tube ' + tube
      @connection.ignore tube, (err) =>
        if err
          @log 'Error ignoring tube : ' + tube
        @ignore_tubes tubes.slice(1), done

    else
      done()


  stop: () ->
    @log 'Stopping...'
    @stopped: true

  find_handler: (job_type) ->
    for handler in @handlers
      if handler[job_type]
        return handler[job_type]
    null

  handle_next: () ->
    if @stopped
      @connection.end()
      @log 'Stopped'
      return

    @connection.reserve_with_timeout 5, (err, job_id, job_json) =>
      if err
        if 'TIMED_OUT' == err
          @emit('next')
        else
          @log 'Error reserving job : ' + err.toString();
      else
        try
          job: JSON.parse(job_json)

        catch e
          job: null
          @log 'Error parsing job JSON : ' + job_id + ' : ' + e.toString()

        if job?
          @handle_job(job_id, job)

        else
          @log 'Error handling job : ' + job_id + ' : couldn\'t parse job : ' + job_json
          @bury_and_emit_next(job_id)

  handle_job: (job_id, job) ->
    handler: @find_handler job.type

    if handler?
      @run_handler_on_job_data(handler, job_id, job.data)

    else
      @log 'Error handling job : ' + job_id + ' : no handler for ' + JSON.stringify(job)
      @bury_and_emit_next(job_id)


  run_handler_on_job_data: (handler, job_id, job_data) ->
    start: new Date().getTime()

    try
      job_canceled: false

      handler job_data, (action, data) =>

        if !job_canceled
          duration: new Date().getTime() - start

          if !action? || ('next' == action)
            @log 'Ran job : ' + job_id + ' in ' + duration + 'ms (' + JSON.stringify(job_data) + ')'
            @destroy_and_emit_next(job_id)

          else if 'release' == action
            job_canceled: true
            @log('Released job : ' + job_id + ' after ' + duration + 'ms')
            @release_and_emit_next(job_id, data)

          else if 'bury' == action
            @log('Buried job : ' + job_id)
            @bury_and_emit_next(job_id)

          else
            @log('Failed to run job : ' + job_id + ' : ' + reason)

    catch ex
      @log 'Exception running job : ' + job_id + ' : ' + ex.toString()
      @bury_and_emit_next(job_id)


  bury_and_emit_next: (job_id) ->
    @connection.bury job_id, client.LOWEST_PRIORITY, (err) =>
      if err
        @log 'Error burying job : ' + job_id + ' : ' + err.toString()
      @emit('next')


  release_and_emit_next: (job_id, delay) ->
    if !delay?
      delay: 30
    @connection.release job_id, client.LOWEST_PRIORITY, delay, (err) =>
      if err
        @log 'Error releasing job : ' + job_id + ' : ' + err.toString()
      @emit('next')


  destroy_and_emit_next: (job_id) ->
    @connection.destroy job_id, (err) =>
      if err
        @log 'Error destroying job : ' + job_id + ' : ' + err.toString()
      @emit('next')


  log: (message) ->
    @logger.log('[ ' + new Date().toString() + ' ] [ ' + process.pid + ' (' + @id + ') ] : ' + message)

exports.BeanstalkWorker: BeanstalkWorker
