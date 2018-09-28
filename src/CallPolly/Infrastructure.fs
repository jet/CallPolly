[<AutoOpen>]
module private CallPolly.Infrastructure

open System
open System.Diagnostics

type TimeSpan with
    /// Converts a tick count as measured by stopwatch into a TimeSpan value
    static member FromStopwatchTicks(ticks : int64) =
        let ticksPerSecond = double Stopwatch.Frequency
        let totalSeconds = double ticks / ticksPerSecond
        TimeSpan.FromSeconds totalSeconds

type Async with

    /// <summary>
    ///     Gets the result of given task so that in the event of exception
    ///     the actual user exception is raised as opposed to being wrapped
    ///     in a System.AggregateException.
    /// </summary>
    /// <param name="task">Task to be awaited.</param>
    [<DebuggerStepThrough>]
    static member AwaitTaskCorrect(task : System.Threading.Tasks.Task<'T>) : Async<'T> =
        Async.FromContinuations(fun (sc,ec,_) ->
            task.ContinueWith(fun (t : System.Threading.Tasks.Task<'T>) ->
                if t.IsFaulted then
                    let e = t.Exception
                    if e.InnerExceptions.Count = 1 then ec e.InnerExceptions.[0]
                    else ec e
                elif t.IsCanceled then ec(new System.Threading.Tasks.TaskCanceledException())
                else sc t.Result)
            |> ignore)