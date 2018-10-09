﻿namespace CallPoly.Tests

open CallPolly
open Swensen.Unquote
open Xunit

/// Base tests exercising core functionality
type Parsing(output : Xunit.Abstractions.ITestOutputHelper) =
    let log = LogHooks.createLogger output

    let [<Fact>] ``Unknown rules are identified`` () : unit =
        let defs = """{ "services": { "default": {
            "calls": {
                "callA": "default"
            },
            "defaultPolicy": null,
            "policies": {
                "default" : [
                    { "rule": "Dunno", "arg": "argh" }
                ]
            }
}}}"""

        let res = Parser.parse defs

        test <@ match [ for w in res.Warnings -> w.serviceName, w.callName, w.unknownRule ] with
                | ["default","callA", jo ] -> string jo.["arg"]="argh"
                | x -> failwithf "%A" x @>

    let [<Fact>] ``Missing defaultPolicy specs are reported`` () =
        let defs = """{ "services": { "default": {
            "calls": {},
            "policies": {}
}}}"""

        raisesWith <@ Parser.parse defs @>
            (fun x -> <@    (string x).Contains "Required property 'defaultPolicy' not found in JSON."
                            && (string x).Contains " Path 'services.default'" @>)

    let [<Fact>] ``Null defaultPolicy is permitted; exceptions are triggered on failed lookups`` () =
        let defs = """{ "services": { "default": {
            "calls": {},
            "defaultPolicy": null,
            "policies": {
            }
}}}"""

        let pol = Parser.parse(defs).CreatePolicy log
        raisesWith <@ pol.Find("default","any") @>
            (fun x -> <@ x.Message.StartsWith "Service 'default' does not define a default call policy" @>)

    let [<Fact>] ``Missing policies are reported`` () =
        let defs = """{ "services": { "default": {
            "calls": {
                "a": "missing"
            },
            "defaultPolicy": "default",
            "policies": {
                "default" : []
            }
}}}"""

        raisesWith <@ Parser.parse defs @>
            (fun x -> <@ x.Message.StartsWith "Service 'default' Call 'a' targets undefined Policy 'missing' (policies: " @>)

    let [<Fact>] ``Missing default targets are reported`` () =
        let defs = """{ "services": { "default": {
            "calls": {},
            "defaultPolicy": "missing",
            "policies": {}
}}}"""

        raisesWith <@ Parser.parse defs @>
            (fun x -> <@ x.Message.StartsWith "Could not find a default policy entitled 'missing' (policies:" @>)

    let [<Fact>] ``Missing include targets are reported`` () =
        let defs = """{ "services": { "default": {
            "calls": {},
            "defaultPolicy": "default",
            "policies": {
                "default": [
                    {"rule": "Include", "policy":"x"}
                ]
            }
}}}"""

        raisesWith <@ Parser.parse defs @>
            (fun x -> <@ x.Message.StartsWith "Include Rule at 'default->(default)->default' refers to undefined Policy 'x' (policies: " @>)

    let [<Fact>] ``Recursive include targets are reported`` () =
        let defs = """{ "services": { "default": {
            "calls": {},
            "defaultPolicy": "pol",
            "policies": {
                "pol": [
                    {"rule": "Include", "policy":"pol"}
                ]
            }
}}}"""

        raisesWith <@ Parser.parse defs @>
            (fun x -> <@ x.Message.StartsWith "Include Rule at 'default->(default)->pol' refers recursively to 'pol' (policies: " @>)

/// Testing derivation of Config info
type ConfigParsing(output : Xunit.Abstractions.ITestOutputHelper) =
    let log = LogHooks.createLogger output

    let [<Fact>] ``Base Uris without trailing slashes are shimmed to prevent last portion before trailing slash being ignored`` () : unit =
        let defs = """{ "services": { "svc": {
            "calls": { "call": "default" },
            "defaultPolicy": null,
            "policies": {
                "default" : [
                    { "rule": "Uri", "base": "https://base/api", "path": "call" }
                ]
            }
}}}"""

        let res = Parser.parse(defs).CreatePolicy log
        let effectiveUri = trap <@ res.TryFind("svc","call").Value.Config |> snd |> Option.get @>
        test <@ "https://base/api/call" = string effectiveUri @>