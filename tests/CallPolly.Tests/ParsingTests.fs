namespace CallPoly.Tests

open CallPolly
open Swensen.Unquote
open Xunit

/// Base tests exercising core functionality
type Parsing(output : Xunit.Abstractions.ITestOutputHelper) =
    let log = LogHooks.createLogger output

    let [<Fact>] ``Missing defaultPolicy specs are reported`` () =
        let defs = """{ "services": { "default": {
            "calls": {},
            "policies": {}
}}}"""

        raisesWith <@ Parser.parse log defs @>
            (fun x -> <@    (string x).Contains "Required property 'defaultPolicy' not found in JSON."
                            && (string x).Contains " Path 'services.default'" @>)

    let [<Fact>] ``Null defaultPolicy is permitted; exceptions are triggered on failed lookups`` () =
        let defs = """{ "services": { "default": {
            "calls": {},
            "defaultPolicy": null,
            "policies": {
            }
}}}"""

        let pol = Parser.parse log defs
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

        raisesWith <@ Parser.parse log defs @>
            (fun x -> <@ x.Message.StartsWith "Service 'default' Call 'a' targets undefined Policy 'missing' (policies: " @>)

    let [<Fact>] ``Missing default targets are reported`` () =
        let defs = """{ "services": { "default": {
            "calls": {},
            "defaultPolicy": "missing",
            "policies": {}
}}}"""

        raisesWith <@ Parser.parse log defs @>
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

        raisesWith <@ Parser.parse log defs @>
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

        raisesWith <@ Parser.parse log defs @>
            (fun x -> <@ x.Message.StartsWith "Include Rule at 'default->(default)->pol' refers recursively to 'pol' (policies: " @>)