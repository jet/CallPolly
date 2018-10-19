module private CallPolly.BulkheadMulti

open Polly
open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

type RefCounted<'T> = { mutable refCount: int; value: 'T }

// via https://stackoverflow.com/a/31194647/11635
type private BulkheadPool(gen : unit -> Bulkhead.BulkheadPolicy) =
    let inners: Dictionary<string, RefCounted<Bulkhead.BulkheadPolicy>> = Dictionary()

    let getOrCreateSlot key =
        lock inners <| fun () ->
            match inners.TryGetValue key with
            | true, inner ->
                inner.refCount <- inner.refCount + 1
                inner.value
            | false, _ ->
                let value = gen ()
                inners.[key] <- { refCount = 1; value = value }
                value
    let slotReleaseGuard key : IDisposable =
        { new System.IDisposable with
            member __.Dispose() =
                lock inners <| fun () ->
                    let item = inners.[key]
                    match item.refCount with
                    | 1 -> inners.Remove key |> ignore
                    | current -> item.refCount <- current - 1 }
    member __.Execute k f = async {
        let x = getOrCreateSlot k
        use _ = slotReleaseGuard k
        return! f x }
    member __.DumpState() : IDictionary<string,Bulkhead.BulkheadPolicy> =
        lock inners <| fun () ->
            dict <| seq { for KeyValue(k, { value = v }) in inners -> k,v }

type BulkheadMultiAsyncPolicy private
    (   tag : string,
        locks: BulkheadPool,
        asyncExecutionPolicy : Func<Func<Context, CancellationToken, Task>, Context, CancellationToken, bool, Task>) =
    inherit Polly.Policy(asyncExecutionPolicy, Seq.empty)
    new(tag, maxParallelization, maxQueuingActions, tryGetTagValue, onBulkheadRejected) =
        let mkBulkheadForTagValue () = Policy.BulkheadAsync(maxParallelization, maxQueuingActions, fun ctx -> onBulkheadRejected ctx; Task.CompletedTask)
        let locks = BulkheadPool(mkBulkheadForTagValue)
        let run (inner: Func<Context,CancellationToken,Task>) (ctx: Context) (ct: CancellationToken) (continueOnCapturedContext: bool) : Task =
            match tryGetTagValue ctx with
            | None -> inner.Invoke(ctx, ct)
            | Some tagVal ->
                let startInnerTask (ctx: Context) (ct: CancellationToken) =
                    inner.Invoke(ctx,ct)
                let executeInner (bulkhead: Polly.Bulkhead.BulkheadPolicy) : Async<unit> =
                    bulkhead.ExecuteAsync(Func<Context,CancellationToken,Task> startInnerTask, ctx, ct, continueOnCapturedContext)
                    |> Async.AwaitTaskCorrect
                Async.StartAsTask(locks.Execute tagVal executeInner, cancellationToken=ct) :> _
        new BulkheadMultiAsyncPolicy(tag, locks, Func<Func<Context,CancellationToken,Task>,Context,CancellationToken,bool,_> run)
    member __.DumpState() : IDictionary<string,Bulkhead.BulkheadPolicy> = locks.DumpState()
    member __.Tag = tag

type Policy =
    /// Placeholder impl of https://github.com/App-vNext/Polly/issues/507
    static member BulkheadMultiAsync(tag, maxParallelization : int, maxQueuingActions : int, tryGetTagValue : Context -> string option, onBulkheadRejected: Context -> unit) =
        BulkheadMultiAsyncPolicy(tag, maxParallelization, maxQueuingActions, tryGetTagValue, onBulkheadRejected)