﻿module CallPolly.Parser

open CallPolly.Rules
open Newtonsoft.Json
open Newtonsoft.Json.Converters
open System.Collections.Generic

/// Wrappers for Newtonsoft.Json
type Newtonsoft() =
    static let settings =
        let tmp = Settings.CreateDefault()
        tmp.Converters.Add(Converters.OptionConverter())
        tmp

    /// <summary>Deserializes value of given type from json string.</summary>
    /// <param name="json">Json string to deserialize.</param>
    static member Deserialize<'T> (json : string) : 'T =
        JsonConvert.DeserializeObject<'T>(json, settings)

/// Defines a set of base call policies for a system, together with a set of service-specific rules
type [<NoComparison; NoEquality>]
    [<JsonObject(ItemRequired = Required.Always)>]
    CallPolicyDefinition =
    {
        /// Defines Call->Policy->Rule mappings per Service of an application
        services: IDictionary<string,CallPolicyServiceDefinition> }

and [<NoComparison; NoEquality>]
    [<JsonObject(ItemRequired = Required.Always)>]
    CallPolicyServiceDefinition =
    {
        /// Map of Call names to Policies to use (may refer to items in `globalPolicies`)
        calls: IDictionary<string,string>

        /// Default Policy to use if lookup fails
        /// - can be from `globalPolicies`)
        /// - can be 'null' to indicate code should throw
        [<JsonProperty(Required=Required.AllowNull)>] // exception: `null` is allowed here
        defaultPolicy: string

        // Map of Policy names to Rule lists specific to this Service
        policies: IDictionary<string,Input[]> }

        // TOCONSIDER it is proposed that any fallback mechanism would be modelled like this
        // /// Reference to name of another ServicePolicy whose policies are to be included in this `policies` map
        // [<JsonProperty(Required=Required.DisallowNull)>] // exception: `null` is allowed here
        // includePoliciesFrom: string
and [<NoComparison>]
    [<RequireQualifiedAccess>]
    [<Newtonsoft.Json.JsonConverter(typeof<UnionConverter>, "rule", "Unknown")>] Input =
    | Include of policy: string
    /// Catch-all case when a ruleName is unknown (allows us to add new policies but have existing instances safely ignore it)
    | Unknown of Newtonsoft.Json.Linq.JObject

    (* Config.Policy.Input.Value *)

    | Isolate
    | Break of Config.Policy.Input.BreakInput
    | Limit of Config.Policy.Input.LimitInput
    | Cutoff of Config.Policy.Input.CutoffInput

    (* Config.Http.Input.Value *)

    | Uri of Config.Http.Input.UriInput
    | Sla of Config.Http.Input.SlaInput
    | Log of Config.Http.Input.LogInput

type PolicyParseException(json, inner) =
    inherit exn(sprintf "Failed to parse CallPolicy: '%s'" json, inner)

[<AbstractClass>]
type CallPolicyReferenceException(msg, ?inner : exn) =
    inherit exn(msg, defaultArg inner null)

type NotFoundCallPolicyReferenceException(service, call, policy, policies) =
    inherit CallPolicyReferenceException(sprintf "Service '%s' Call '%s' targets undefined Policy '%s' (policies: %s)" service call policy policies)

type RecursiveIncludeCallPolicyReferenceException(stack, policy, policies) =
    inherit CallPolicyReferenceException(sprintf "Include Rule at '%s' refers recursively to '%s' (policies: %s)" stack policy policies)

type IncludeNotFoundCallPolicyReferenceException(stack, policy, policies) =
    inherit CallPolicyReferenceException(sprintf "Include Rule at '%s' refers to undefined Policy '%s' (policies: %s)" stack policy policies)

module Constants =
    let [<Literal>] defaultCallName = "(default)"

[<NoComparison>]
[<RequireQualifiedAccess>]
type Parsed =
    | Policy of Config.Policy.Input.Value
    | Http of Config.Http.Input.Value
    | Unknown of Newtonsoft.Json.Linq.JObject

let parseInternal defsJson =
    let defs: CallPolicyDefinition = try Newtonsoft.Deserialize defsJson with e -> PolicyParseException(defsJson, e) |> raise
    let dumpDefs (serviceDef: CallPolicyServiceDefinition) = sprintf "%A" [ for x in serviceDef.policies -> x.Key, List.ofArray x.Value ]
    let rec mapService stack (serviceDef : CallPolicyServiceDefinition) polRules : Parsed seq =
        let rec unroll stack (def : Input) : Parsed seq = seq {
            match def with
            | Input.Uri x -> yield Parsed.Http (Config.Http.Input.Value.Uri x)
            | Input.Sla x -> yield Parsed.Http (Config.Http.Input.Value.Sla x)
            | Input.Log x -> yield Parsed.Http (Config.Http.Input.Value.Log x)

            | Input.Isolate -> yield Parsed.Policy Config.Policy.Input.Value.Isolate
            | Input.Break x -> yield Parsed.Policy (Config.Policy.Input.Value.Break x)
            | Input.Limit x -> yield Parsed.Policy (Config.Policy.Input.Value.Limit x)
            | Input.Cutoff x -> yield Parsed.Policy (Config.Policy.Input.Value.Cutoff x)

            | Input.Unknown x -> yield Parsed.Unknown x
            | Input.Include includedPolicyName ->
                match stack |> List.tryFindIndex (fun x -> x=includedPolicyName) with
                | None -> ()
                | Some x when x >= List.length stack - 1 -> () // can refer to serviceName or callName as long as it's not also a policyName
                | _ -> raise <| RecursiveIncludeCallPolicyReferenceException(String.concat "->" (List.rev stack), includedPolicyName, dumpDefs serviceDef)

                match serviceDef.policies.TryGetValue includedPolicyName with
                | false, _ -> raise <| IncludeNotFoundCallPolicyReferenceException(String.concat "->" (List.rev stack), includedPolicyName, dumpDefs serviceDef)
                | true, defs ->
                    let stack' = includedPolicyName :: stack
                    yield! defs |> Seq.collect (unroll stack') }
        polRules |> Seq.collect (unroll stack)

    seq { for KeyValue (serviceName,serviceDef) in defs.services ->
            let mapCall callName policyName =
                match serviceDef.policies.TryGetValue policyName with
                | false, _ -> raise <| NotFoundCallPolicyReferenceException(serviceName, callName, policyName, dumpDefs serviceDef)
                | true, rules ->
                    let mapped = mapService [policyName; callName; serviceName] serviceDef rules |> List.ofSeq
                    callName,
                    {   policyName = policyName
                        policyConfig = mapped |> Seq.choose (function Parsed.Policy x -> Some x | _ -> None) |> Config.Policy.ofInputs
                        callConfig = mapped |> Seq.choose (function Parsed.Http x -> Some x | _ -> None) |> Config.Http.ofInputs
                        raw = mapped }
            let defaultCallName, callsMap =
                let explicitCalls = [ for KeyValue (callName,policyName) in serviceDef.calls -> mapCall callName policyName]
                match Option.ofObj serviceDef.defaultPolicy with
                | Some defPolName when not (serviceDef.policies.ContainsKey defPolName) ->
                    invalidOp (sprintf "Could not find a default policy entitled '%s' (policies: %s)" defPolName (dumpDefs serviceDef))
                | Some _ when serviceDef.calls.ContainsKey Constants.defaultCallName ->
                    invalidOp (sprintf "Service '%s' may not define a Call with the reserved name '%s' when a 'defaultPolicy' is declared" serviceName Constants.defaultCallName)
                | Some defPolName ->
                    Some Constants.defaultCallName, mapCall Constants.defaultCallName defPolName :: explicitCalls
                | _ ->
                    None, explicitCalls
            serviceName, { serviceName = serviceName; defaultPolicyName = defaultCallName; callsMap = callsMap } }

let parse log defs : Policy<_,_> =
    let rulesMap = parseInternal defs
    Policy.Create(log, rulesMap)

let updateFrom defs (x : Policy<_,_>) : (string * (string * ChangeLevel) list) list =
    let rulesMap = parseInternal defs
    x.UpdateFrom rulesMap