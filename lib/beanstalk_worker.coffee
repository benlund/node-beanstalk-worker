events = require 'events'
client = require('beanstalk_client').Client

class BeanstalkWorker extends events.EventEmitter

  constructor: (id, server, handlers, logger) ->
    @id = id
    @server = server
    @handlers = handlers
    @logger = logger || console

    @stopped = false


  start: (tubes, ignore_default) ->
    this.log 'Starting...'

    this.on 'next', () =>
      this.handle_next()

    client.connect @server, (err, conn) =>
      if err
        this.log 'Error connecting: ' + err
      else
        @connection = conn

        this.watch_tubes tubes, () =>
          ignored = []
          if ignore_default
          	ignored.push 'default'

          this.ignore_tubes ignored, () =>
          	this.log 'Started'
          	this.emit 'next'


  watch_tubes: (tubes, done) ->
    if tubes? && (tube = tubes[0])
      this.log 'Watching tube ' + tube
      this.connection.watch tube, (err) =>
        if err
          this.log 'Error watching tube : ' + tube
        this.watch_tubes tubes.slice(1), done

    else
      done()


  ignore_tubes: (tubes, done) ->
    if tubes? && (tube = tubes[0])
      this.log 'Ignoring tube ' + tube
      this.connection.ignore tube, (err) =>
        if err
          this.log 'Error ignoring tube : ' + tube
        this.ignore_tubes tubes.slice(1), done

    else
      done()


  stop: () ->
    this.log 'Stopping...'
    @stopped = true

  find_handler: (job_type) ->
    for handler in @handlers
      if handler[job_type]
        return handler[job_type]
    null

  handle_next: () ->
    if @stopped
      @connection.end()
      this.log 'Stopped'
      return

    @connection.reserve_with_timeout 5, (err, job_id, job_json) =>
      if err
        if 'TIMED_OUT' == err
          this.emit 'next'
        else
          this.log 'Error reserving job : ' + err.toString();
      else
        try
          job = JSON.parse(job_json)

        catch e
          job = null
          this.log 'Error parsing job JSON : ' + job_id + ' : ' + e.toString()

        if job?
          this.handle_job(job_id, job)

        else
          this.log 'Error handling job : ' + job_id + ' : couldn\'t parse job : ' + job_json
          this.bury_and_emit_next(job_id)

  handle_job: (job_id, job) ->
    handler = this.find_handler job.type

    if handler?
      this.run_handler_on_job_data(handler, job_id, job.data)

    else
      this.log 'Error handling job : ' + job_id + ' : no handler for ' + JSON.stringify(job)
      this.bury_and_emit_next(job_id)


  run_handler_on_job_data: (handler, job_id, job_data) ->
    start = new Date().getTime()

    try
      job_canceled = false

      handler job_data, (action, data) =>

        if !job_canceled
          duration = new Date().getTime() - start

          if !action? || ('next' == action)
            this.log 'Ran job : ' + job_id + ' in ' + duration + 'ms (' + JSON.stringify(job_data) + ')'
            this.destroy_and_emit_next(job_id)

          else if 'release' == action
            job_canceled = true
            this.log('Released job : ' + job_id + ' after ' + duration + 'ms')
            this.release_and_emit_next(job_id, data)

          else if 'bury' == action
            this.log 'Buried job : ' + job_id
            this.bury_and_emit_next(job_id)

          else
            this.log 'Failed to run job : ' + job_id + ' : ' + reason

    catch ex
      this.log 'Exception running job : ' + job_id + ' : ' + ex.toString()
      this.bury_and_emit_next(job_id)


  bury_and_emit_next: (job_id) ->
    @connection.bury job_id, client.LOWEST_PRIORITY, (err) =>
      if err
        this.log 'Error burying job : ' + job_id + ' : ' + err.toString()
      this.emit 'next'


  release_and_emit_next: (job_id, delay) ->
    if !delay?
      delay = 30
    @connection.release job_id, client.LOWEST_PRIORITY, delay, (err) =>
      if err
        this.log 'Error releasing job : ' + job_id + ' : ' + err.toString()
      this.emit 'next'


  destroy_and_emit_next: (job_id) ->
    @connection.destroy job_id, (err) =>
      if err
        this.log 'Error destroying job : ' + job_id + ' : ' + err.toString()
      this.emit 'next'


  log: (message) ->
    @logger.log('[ ' + new Date().toString() + ' ] [ ' + process.pid + ' (' + @id + ') ] : ' + message)

exports.BeanstalkWorker = BeanstalkWorker
