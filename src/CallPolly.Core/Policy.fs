namespace CallPolly

open System
open System.Collections.Concurrent
open System.Collections.Generic

type [<RequireQualifiedAccess>] ChangeLevel = Added | ConfigurationAndPolicy | Configuration | Policy

type CallConfig<'TCallConfig> = { policyName: string; policy: Governor.PolicyConfig; config: 'TCallConfig }

/// Holds a policy and configuration for a Service Call, together with the state required to govern the incoming calls
type CallPolicy<'TConfig when 'TConfig: equality>(makeGoverner : CallConfig<'TConfig> -> Governor.Governor, cfg : CallConfig<'TConfig>) =
    let mutable cfg, governor = cfg, makeGoverner cfg

    /// Ingest an updated set of config values, reporting diffs, if any
    member __.TryUpdate(updated : CallConfig<'TConfig>) =
        let level =
            match updated.policy = cfg.policy, updated.config = cfg.config with
            | false, false -> Some ChangeLevel.ConfigurationAndPolicy
            | true, false -> Some ChangeLevel.Configuration
            | false, true -> Some ChangeLevel.Policy
            | true, true -> None

        match level with
        | Some ChangeLevel.ConfigurationAndPolicy | Some ChangeLevel.Policy ->
            governor <- makeGoverner updated
            cfg <- updated
        | Some ChangeLevel.Configuration ->
            cfg <- updated
        | _ -> ()

        level

    member __.Policy = cfg.policy
    member __.Config = cfg.config

    /// Execute the call, apply the policy rules
    member __.Execute(inner : Async<'t>, ?log, ?tags) =
        governor.Execute(inner, ?log=log, ?tags=tags)

    /// Facilitates dumping for diagnostics
    member __.InternalState =
        cfg, governor.InternalState

type ServiceConfig<'TConfig> =
    {   defaultCallName: string option
        serviceName: string
        callsMap: (string*CallConfig<'TConfig>) list }

/// Maintains a set of call policies for a service
type ServicePolicy<'TConfig when 'TConfig: equality> private
    (   makeCallPolicy : (*callName:*) string -> CallConfig<'TConfig> -> CallPolicy<'TConfig>,
        defaultCallName : string option,
        calls: ConcurrentDictionary<(*callName:*) string,CallPolicy<'TConfig>>) =
    let mutable defaultCallName = defaultCallName

    new (makeCallPolicy, cfg: ServiceConfig<'TConfig>) =
        let inputs = seq {
            for callName,callCfg in cfg.callsMap ->
                KeyValuePair(callName, makeCallPolicy callName callCfg) }
        ServicePolicy<_>(makeCallPolicy, cfg.defaultCallName, ConcurrentDictionary inputs)

    /// Searches for the CallPolicy associated with `call`
    member __.TryFind(callName : string) =
        match calls.TryGetValue callName with
        | true, callPolicy -> Some callPolicy
        | false, _ -> None

    /// Searches for the default CallPolicy defined for this service (if any)
    member __.TryDefaultPolicy() : CallPolicy<'TConfig> option =
        match defaultCallName with
        | None -> None
        | Some def ->
            match calls.TryGetValue def with
            | true, callPolicy -> Some callPolicy
            | false, _ -> None

    /// Processes as per Parse, but updates an existing ruleset, annotating any detected changes
    member __.UpdateFrom(cfg: ServiceConfig<'TConfig>) =
        let updates = [
            for callName,cfg in cfg.callsMap do
                let mutable changeLevel = None
                let createCallPolicy (callName : string) : CallPolicy<'TConfig> =
                    changeLevel <- Some ChangeLevel.Added
                    makeCallPolicy callName cfg
                let tryUpdate _ (current:CallPolicy<'TConfig>) =
                    changeLevel <- current.TryUpdate(cfg)
                    current
                calls.AddOrUpdate(callName, createCallPolicy, tryUpdate) |> ignore
                match changeLevel with
                | None -> ()
                | Some changeLevel -> yield callName, changeLevel ]
        defaultCallName <- cfg.defaultCallName
        updates

    /// Facilitates dumping for diagnostics
    member __.InternalState = seq {
        for KeyValue(callName, call) in calls ->
            callName, call.InternalState
    }

/// Maintains an application-wide set of service policies, together with their policies, configuration and policy-state
type Policy<'TConfig when 'TConfig : equality> private (makeCallPolicy, services: ConcurrentDictionary<string,ServicePolicy<'TConfig>>) =

    /// Initialize the policy from an initial set of configs
    new(log, createFailurePolicyBuilder : CallConfig<'TConfig> -> Polly.PolicyBuilder, cfgs: ((*service:*)string * ServiceConfig<'TConfig>) seq) =
        let makeGovernor serviceName callName (callConfig: CallConfig<_>) : Governor.Governor =
            Governor.Governor(log, (fun () -> createFailurePolicyBuilder callConfig), serviceName, callName, callConfig.policyName, callConfig.policy)
        let makeCallPolicy serviceName callName (callConfig: CallConfig<_>) : CallPolicy<'TConfig> =
            CallPolicy(makeGovernor serviceName callName, callConfig)
        let inputs = seq {
            for service, cfg in cfgs ->
                KeyValuePair(service, ServicePolicy<_>(makeCallPolicy cfg.serviceName, cfg)) }
        Policy<'TConfig>(makeCallPolicy, ConcurrentDictionary<_,_> inputs)

    /// Processes as per Parse, but updates an existing ruleset, annotating any detected changes
    member __.UpdateFrom(cfgs: ((*callName:*) string * ServiceConfig<_>) seq) =
        let updates = ResizeArray()
        for callName, cfg in cfgs do
            let createServicePolicy _callName =
                ServicePolicy(makeCallPolicy cfg.serviceName, cfg)
            let tryUpdate (current : ServicePolicy<_>) =
                updates.Add(callName, current.UpdateFrom cfg)
                current
            services.AddOrUpdate(callName, createServicePolicy, fun _ current -> tryUpdate current) |> ignore
        updates.ToArray() |> List.ofArray

    /// Attempts to find the policy for the specified `call` in the specified `service`
    member __.TryFind(service, call) : CallPolicy<_> Option =
        match services.TryGetValue service with
        | false, _ -> None
        | true, sp ->
            match sp.TryFind call with
            | Some cp -> Some cp
            | None -> None

    /// Finds the policy for the specified `call` in the specified `service`
    /// Throws InvalidOperationException if `service` not found
    /// Throws InvalidOperationException if `call` not defined in ServicePolicy and no default CallPolicy nominated
    member __.Find(service, call) : CallPolicy<_> =
        match services.TryGetValue service with
        | false, _ -> invalidOp <| sprintf "Undefined service '%s'" service
        | true, sp ->
            match sp.TryFind call with
            | Some cp -> cp
            | None ->
                match sp.TryDefaultPolicy() with
                | None -> invalidOp <| sprintf "Service '%s' does not define a default call policy" service
                | Some dp -> dp

    /// Facilitates dumping all Service Configs for diagnostics
    member __.InternalState = seq {
        for KeyValue(serviceName, service) in services ->
            serviceName, service.InternalState }

type RuleDefinition =
    | Break of Governor.BreakerConfig
    | Limit of Governor.BulkheadConfig
    | LimitBy of Governor.TaggedBulkheadConfig
    | Cutoff of Governor.CutoffConfig
    | Isolate
[<NoComparison>]
type CallDefinition<'TConfig when 'TConfig : equality> = { callName: string; policyName: string; rules: RuleDefinition list; config: 'TConfig }
[<NoComparison>]
type ServiceDefinition<'TConfig when 'TConfig : equality> = { serviceName: string; defaultCallName: string option; calls: CallDefinition<'TConfig> list }

type PolicyBuilder<'TConfig when 'TConfig : equality>(services: ServiceDefinition<'TConfig> seq) =
    let mkPolicyConfig : RuleDefinition seq -> Governor.PolicyConfig =
        let folder (s : Governor.PolicyConfig) = function
            | RuleDefinition.Isolate -> { s with isolate = true }
            | RuleDefinition.Break breakerConfig -> { s with breaker = Some breakerConfig }
            | RuleDefinition.Limit bulkheadConfig -> { s with limit = Some bulkheadConfig }
            | RuleDefinition.LimitBy taggedBulkheadConfig -> { s with taggedLimits = s.taggedLimits @ [taggedBulkheadConfig] }
            | RuleDefinition.Cutoff cutoffConfig -> { s with cutoff = Some cutoffConfig }
        Seq.fold folder { isolate = false; cutoff = None; limit = None; taggedLimits = []; breaker = None }
    let mapService (x : ServiceDefinition<'TConfig>) : (*serviceName:*) string * ServiceConfig<'TConfig> =
        x.serviceName,
        {   serviceName = x.serviceName
            defaultCallName = x.defaultCallName
            callsMap =
                [ for call in x.calls ->
                    let callConfig : CallConfig<_> = { policyName = call.policyName; policy = mkPolicyConfig call.rules; config = call.config }
                    call.callName, callConfig ] }
    let mapped = services |> Seq.map mapService

    member __.Build(log, ?createFailurePolicyBuilder : CallConfig<_> -> Polly.PolicyBuilder) : Policy<_> =
        let createFailurePolicyBuilder =
            match createFailurePolicyBuilder with
            | None -> fun _callConfig -> Polly.Policy.Handle<TimeoutException>()
            | Some custom -> custom
        Policy<'TConfig>(log, createFailurePolicyBuilder, mapped)

    member __.Update(x : Policy<_>) : (string * (string * ChangeLevel) list) list =
        x.UpdateFrom mapped