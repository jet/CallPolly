[<AutoOpen>]
module private CallPolly.Infrastructure

open System.Diagnostics

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