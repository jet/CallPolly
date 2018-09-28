[<AutoOpen>]
module CallPolly.Infrastructure

open System
open System.Diagnostics
open CallPolly.Events // StopwatchInterval

type Async with
    static member Sleep(x : TimeSpan) = Async.Sleep(int x.TotalMilliseconds)

    /// <summary>
    ///     Gets the result of given task so that in the event of exception
    ///     the actual user exception is raised as opposed to being wrapped
    ///     in a System.AggregateException.
    /// </summary>
    /// <param name="task">Task to be awaited.</param>
    [<DebuggerStepThrough>]
    static member AwaitTaskCorrect(task : System.Threading.Tasks.Task) : Async<unit> =
        Async.FromContinuations(fun (sc,ec,_) ->
            task.ContinueWith(fun (task : System.Threading.Tasks.Task) ->
                if task.IsFaulted then
                    let e = task.Exception
                    if e.InnerExceptions.Count = 1 then ec e.InnerExceptions.[0]
                    else ec e
                elif task.IsCanceled then
                    ec(System.Threading.Tasks.TaskCanceledException())
                else
                    sc ())
            |> ignore)

    /// Helper function for testing only - does a Sleep without honoring cancellation as F# stdlib did pre 3.0
    // See https://stackoverflow.com/a/14149643/11635 for some discussion and examples
    static member SleepWrong(x : TimeSpan) =
        System.Threading.Tasks.Task.Delay(x (* sic - not propagating Async.CancellationToken*)) |> Async.AwaitTaskCorrect

type Stopwatch =
    /// <summary>
    ///     Times an async computation, returning the result with a time range measurement.
    /// </summary>
    /// <param name="f">Function to execute & time.</param>
    [<DebuggerStepThrough>]
    static member Time(f : Async<'T>) : Async<StopwatchInterval * 'T> = async {
        let startTicks = System.Diagnostics.Stopwatch.GetTimestamp()
        let! result = f
        let endTicks = System.Diagnostics.Stopwatch.GetTimestamp()
        let tr = StopwatchInterval(startTicks, endTicks)
        return tr, result
    }

[<RequireQualifiedAccess>]
module Choice =
    /// Splits a set of inputs into Choice1Of2 / Choice2Of2
    let partition (inputs : seq<Choice<'Left,'Right>>) =
        let left, right = ResizeArray(), ResizeArray()
        for i in inputs do
            match i with
            | Choice1Of2 l -> left.Add l
            | Choice2Of2 r -> right.Add r
        left.ToArray(), right.ToArray()
    /// Splits a set of inputs into Choice1Of3 / Choice2Of3 / Choice3Of3
    let partition3 (inputs : seq<Choice<'Left,'Middle,'Right>>) =
        let left, middle, right = ResizeArray(), ResizeArray(), ResizeArray()
        for i in inputs do
            match i with
            | Choice1Of3 l -> left.Add l
            | Choice2Of3 m -> middle.Add m
            | Choice3Of3 r -> right.Add r
        left.ToArray(), middle.ToArray(), right.ToArray()

[<AutoOpen>]
module SerilogHelpers =
    open Serilog
    open Serilog.Events

    /// Create a logger, targeting the specified outputs
    type LogOutput = LocalSeq | Sink of Serilog.Core.ILogEventSink
    let createLoggerWithSinks outputs =
        let addSink (logger : Serilog.LoggerConfiguration) = function
            | Sink sink -> logger.WriteTo.Sink(sink)
            | LocalSeq -> logger.WriteTo.Seq("http://localhost:5341")
        let sinksConfig = (Serilog.LoggerConfiguration(), outputs) ||> List.fold addSink
        sinksConfig .CreateLogger()

    // Derived from https://github.com/damianh/CapturingLogOutputWithXunit2AndParallelTests
    type TestOutputAdapter(testOutput : Xunit.Abstractions.ITestOutputHelper) =
        let formatter = Serilog.Formatting.Display.MessageTemplateTextFormatter("{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level}] {Message}{NewLine}{Exception}", null);
        let writeSerilogEvent logEvent =
            use writer = new System.IO.StringWriter()
            formatter.Format(logEvent, writer)
            writer |> string |> testOutput.WriteLine
        interface Serilog.Core.ILogEventSink with member __.Emit logEvent = writeSerilogEvent logEvent
    type SerilogTraceSink() =
        let writeSerilogEvent (logEvent: Serilog.Events.LogEvent) =
            logEvent.RenderMessage () |> System.Diagnostics.Trace.WriteLine
        interface Serilog.Core.ILogEventSink with member __.Emit logEvent = writeSerilogEvent logEvent

    type LogCaptureBuffer() =
        let captured = ResizeArray()
        let capture (logEvent: LogEvent) =
            captured.Add logEvent
        interface Serilog.Core.ILogEventSink with member __.Emit logEvent = capture logEvent
        member __.Clear () = captured.Clear()
        member __.Entries = captured.ToArray()

    let (|SerilogScalar|_|) : Serilog.Events.LogEventPropertyValue -> obj option = function
        | (:? ScalarValue as x) -> Some x.Value
        | _ -> None
    let (|HasProp|_|) (name : string) (e : LogEvent) : LogEventPropertyValue option =
        match e.Properties.TryGetValue name with
        | true, (SerilogScalar _ as s) -> Some s | _ -> None
        | _ -> None
    let (|TemplateContains|_|) (substr : string) (e : LogEvent) =
        if e.MessageTemplate.Text.Contains substr then Some ()
        else None
    let (|SerilogString|_|) : LogEventPropertyValue -> string option = function SerilogScalar (:? string as y) -> Some y | _ -> None
    let (|SerilogBool|_|) : LogEventPropertyValue -> bool option = function SerilogScalar (:? bool as y) -> Some y | _ -> None

    let dumpEvent (x : LogEvent) =
        let formatter = Serilog.Formatting.Display.MessageTemplateTextFormatter("{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level}] {Message}{NewLine}{Exception}|{Properties}", null);
        use writer = new System.IO.StringWriter()
        formatter.Format(x, writer)
        string writer

module LogHooks =
    let defaultLogOutputs = [
#if DEBUG // Seq is licensed for a single developer to self-host - downloading will enable rich viewing of logs, see http://getseq.net
        LocalSeq
#endif
        Sink (SerilogTraceSink()) ]

    /// Create a logger that does not capture events, but sends copies to standard debugging outputs
    let createLogger testOutputHelper =
        createLoggerWithSinks (Sink (TestOutputAdapter testOutputHelper) :: defaultLogOutputs)

    /// Create a logger that captures a copy of each LogEvent for verification within the test
    let createLoggerWithCapture testOutputHelper =
        let capture = LogCaptureBuffer()
        let sinks = Sink capture :: Sink (TestOutputAdapter testOutputHelper) :: defaultLogOutputs
        let logger = createLoggerWithSinks sinks
        logger, capture