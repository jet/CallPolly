namespace CallPoly.Tests

open CallPolly
open Swensen.Unquote
open Xunit

/// Base tests exercising core functionality
type Parsing(output : Xunit.Abstractions.ITestOutputHelper) =
    let log = LogHooks.createLogger output

    let [<Fact>] ``Missing policies are reported`` () =
        let pols = """{
            "pol": []
        }"""
        let map = """
            {"a":"missing"}
        """

        raisesWith <@ Parser.parseUpstreamPolicyWithoutDefault log pols map @>
            (fun x -> <@ x.Message.StartsWith "Rule for Action 'a' targets undefined Policy 'missing' (policies:" @>)

    let [<Fact>] ``Missing defaults are reported`` () =
        let pols = """{
            "pol": []
        }"""
        let map = """{}
        """

        raisesWith <@ Parser.parseUpstreamPolicy log pols map @>
            (fun x -> <@ x.Message.StartsWith "Could not find a default policy entitled '(default)'" @>)

    let [<Fact>] ``Missing default targets are reported`` () =
        let pols = """{
            "pol": []
        }"""
        let map = """
            {"(default)":"missing"}
        """

        raisesWith <@ Parser.parseUpstreamPolicy log pols map @>
            (fun x -> <@ x.Message.StartsWith "Rule for Action '(default)' targets undefined Policy 'missing' (policies:" @>)

    let [<Fact>] ``Missing include targets are reported`` () =
        let pols = """{
            "pol": [{"ruleName": "Include", "policyName":"x"}]
        }"""
        let map = """
            {"(default)":"pol"}
        """

        raisesWith <@ Parser.parseUpstreamPolicy log pols map @>
            (fun x -> <@ x.Message.StartsWith "Include Rule for Policy '(default)->pol' refers to undefined Policy 'x' (policies: " @>)

    let [<Fact>] ``Recursive include targets are reported`` () =
        let pols = """{
            "pol": [{"ruleName": "Include", "policyName":"pol"}]
        }"""
        let map = """
            {"(default)":"pol"}
        """

        raisesWith <@ Parser.parseUpstreamPolicy log pols map @>
            (fun x -> <@ x.Message.StartsWith "Include Rule '(default)->pol' refers recursively to 'pol' (policies: " @>)