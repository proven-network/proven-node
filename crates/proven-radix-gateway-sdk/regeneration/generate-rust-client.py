#!/usr/bin/env python3

import urllib.request, logging, subprocess, os, shutil, re, yaml, urllib, json

# Inspired by https://github.com/radixdlt/radixdlt-python-clients
# Requires python 3+ and `pip install pyyaml`

logger = logging.getLogger()
logging.basicConfig(format='%(asctime)s [%(levelname)s]: %(message)s', level=logging.INFO)

API_SCHEMA_URL = 'https://raw.githubusercontent.com/radixdlt/babylon-gateway/main/src/RadixDlt.NetworkGateway.GatewayApi/gateway-api-schema.yaml'
API_SCHEMA_LOCATION='./temp/gateway-api-schema.yaml'
API_GENERATED_DESTINATION = '../src/generated'

OPENAPI_GENERATION_FOLDER='.'
OPENAPI_TEMP_GENERATION_FOLDER='./temp'

def safe_os_remove(path, silent = False):
    try:
        shutil.rmtree(path) if os.path.isdir(path) else os.remove(path)
    except Exception as e:
        if not silent: logger.warning(e)

def replace_in_file(filename, target, replacement):
    with open(filename, 'r') as file:
        file_contents = file.read()
    file_contents = file_contents.replace(target, replacement)
    with open(filename, 'w') as file:
        file.write(str(file_contents))

def find_in_file_multiline(filename, regex):
    with open(filename, 'r') as file:
        file_contents = file.read()
    return re.findall(regex, file_contents)

def create_file(filename, file_contents):
    with open(filename, 'w') as file:
        file.write(str(file_contents))

def copy_file(source, dest):
    shutil.copyfile(source, dest)

def run(command, cwd = '.', should_log = False, ignore_errors = False):
    if (should_log): logging.debug('Running cmd: %s' % command)
    response = subprocess.run(' '.join(command), cwd=cwd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stderr = response.stderr.decode('utf-8')
    if not ignore_errors and response.returncode != 0: raise Exception(stderr)
    stdout = response.stdout.decode('utf-8')
    if (should_log): logging.debug('Response: %s', stdout)
    return stdout

def split_out_inherited_discriminated_types(schema):
    """
    Some context:

    The Open API specificaiton is very unclear how to handle discriminated unions, leaving different languages
    and code generators to implement it in different ways.

    The "best" way to do it which allows class-based languages with inheritance to work right is to have allOf on the
    parent class, and have various child classes use anyOd, like this: https://redocly.com/docs/resources/discriminator/#Vehicle.yaml

    BUT many code generators - such as:
    - The TypeScript "open api generator"
    - Ovral - a specialized typescript generator

    Do not support it properly - in particular, to avoid recursive type definitions, you need to essentially add in a "Base" type.
    This method introduces this Base type so that the type generation works correctly.
    """
    types = schema["components"]["schemas"]
    typeRefsChanged = {}
    originalTypeNames = list(types.keys())

    # First pass - we need to find all the types that are discriminated unions, and split them
    for typeName in originalTypeNames:
        typeData = types[typeName]
        if "discriminator" in typeData and "type" in typeData and typeData["type"] == "object" and not ("allOf" in typeData or "anyOf" in typeData or "oneOf" in typeData):
            # Assume it's one of the types we're targetting!

            baseTypeData = {
                typeDataKey: typeData[typeDataKey] for typeDataKey in typeData if typeDataKey != "discriminator"
            }

            unionTypeData = {
                # Technically speaking it should be `anyOf` here because the child type doesn't validate the exact key...
                # And so multiple child types could be valid at the same time.
                # But this results in the typescript generator outputting invalid code which doesn't compile (yay).
                # So we stick with "oneOf".
                "oneOf": [
                    { "$ref": childTypeReference } for childTypeReference in typeData["discriminator"]["mapping"].values()
                ],
                "discriminator": typeData["discriminator"],
            }

            baseTypeName = typeName + "Base"
            while baseTypeName in types: # Very basic collision avoidance
                baseTypeName = baseTypeName + "Derived"

            typeRef = "#/components/schemas/" + typeName
            baseTypeRef = "#/components/schemas/" + baseTypeName
            typeRefsChanged[typeRef] = {
                "baseTypeRef": baseTypeRef,
                "discriminator": typeData["discriminator"],
            }

            types[typeName] = unionTypeData
            types[baseTypeName] = baseTypeData

    # Second pass - we go through all the other types, and update their references to the new types
    for typeName in originalTypeNames:
        typeData = types[typeName]
        if "allOf" in typeData and isinstance(typeData["allOf"], list):
            match = None
            for inheritedType in typeData["allOf"]:
                if "$ref" in inheritedType and inheritedType["$ref"] in typeRefsChanged:
                    parentTypeDetails = typeRefsChanged[inheritedType["$ref"]]
                    inheritedType["$ref"] = parentTypeDetails["baseTypeRef"]
                    match = parentTypeDetails
            if match is not None:
                mapping = match["discriminator"]["mapping"]
                ownTag = next((tag for tag in mapping if mapping[tag] == "#/components/schemas/" + typeName), None)
                if ownTag is not None:
                    # Either find the existing inline inner type to amend - or add one
                    lastInnerTypeData = next((
                        innerTypeData for innerTypeData in typeData["allOf"]
                            if "type" in innerTypeData and innerTypeData["type"] == "object"
                            and "properties" in innerTypeData and isinstance(innerTypeData["properties"], dict)
                    ), None)
                    if lastInnerTypeData is None:
                        lastInnerTypeData = { "type": "object", "properties": {} }
                        typeData["allOf"].append(lastInnerTypeData)

                    # Restrict the type of the property name to match the discriminator
                    lastInnerTypeData["properties"][match["discriminator"]["propertyName"]] = {
                        "type": "string",
                        "enum": [ownTag],
                    }


def prepare_schema_for_generation(original_schema_file, api_schema_temp_filename):
    with open(original_schema_file, 'r') as file:
        schema = yaml.safe_load(file)

    split_out_inherited_discriminated_types(schema)

    with open(api_schema_temp_filename, 'w') as file:
        yaml.dump(schema, file, sort_keys=False)

def generate_models(prepared_spec_file, tmp_client_folder, out_location):
    safe_os_remove(tmp_client_folder, True)

    run(['libninja', 'gen', '--lang', 'rust', '--repo', 'borderline-labs/radix-gateway-sdk', '--derive', 'Clone', '-o', tmp_client_folder, 'LowLevel', prepared_spec_file], should_log=False)

    logging.info("Successfully generated.")

    logging.info("Applying patches...")

    for root, dirs, files in os.walk(os.path.join(tmp_client_folder, "src")):
        for file in files:
            if file.endswith(".rs"):
                # Go through every file in generated src folder and replace "use crate::" with "use crate::generated" inside all .rs files
                replace_in_file(os.path.join(root, file), "use crate::", "use crate::generated::")
                # Change the output results of requests to be crate level one
                replace_in_file(os.path.join(root, file), "type Output = httpclient::InMemoryResult", "type Output = crate::Result")
                replace_in_file(os.path.join(root, file), "res.json().map_err(Into::into)", "res.json().map_err(|e| crate::Error::LowLevel(e.into()))")
                # Fix strange naming of ResourceChange as ResourceChang
                replace_in_file(os.path.join(root, file), "ResourceChang", "ResourceChange")
                replace_in_file(os.path.join(root, file), "resource_chang", "resource_change")
                replace_in_file(os.path.join(root, file), "InstructionResourceChangees", "InstructionResourceChanges")
                # Go through each line and if Clone trait is found twice, remove the second one
                count = find_in_file_multiline(os.path.join(root, file), ", Clone")
                if len(count) > 1:
                    replace_in_file(os.path.join(root, file), ", Clone", "")
                # Append newline to the end of the file
                with open(os.path.join(root, file), 'a') as f:
                    f.write("\n")

    # Fix resource_change.rs path
    copy_file(os.path.join(tmp_client_folder, "src", "model", "resource_chang.rs"), os.path.join(tmp_client_folder, "src", "model", "resource_change.rs"))
    safe_os_remove(os.path.join(tmp_client_folder, "src", "model", "resource_chang.rs"), silent=True)

    lib_path = os.path.join(tmp_client_folder, "src", "lib.rs")
    mod_path = os.path.join(tmp_client_folder, "src", "mod.rs")

    # Remove auth parameter
    replace_in_file(lib_path, ", authentication: LowLevelAuth", "")
    # Remove any lines with "authentication" from mod.rs
    with open(lib_path, 'r') as file:
        lines = file.readlines()
        with open(mod_path, 'w') as file:
            for line in lines:
                if "pub struct LowLevelClient" in line:
                    file.write("#[derive(Default, Debug)]\n")
                if "authentication" not in line:
                    file.write(line)

    safe_os_remove(lib_path, silent=True)

    safe_os_remove(out_location, silent=True)
    shutil.copytree(os.path.join(tmp_client_folder, "src"), out_location)
    safe_os_remove(tmp_client_folder, silent=True)

    # This is a workaround for the issue with the generated code where the Default trait is not satisfied by a child struct
    # TODO: Should probably perform this in the temp folder instead of the final destination
    while True:
        check_output = run(['cargo', 'check', '--message-format=json', '-q'], ignore_errors = True)
        if "Default` is not satisfied" not in check_output:
            break
        for line in check_output.splitlines():
            if "Default` is not satisfied" in line:
                json_output = json.loads(line)
                target_file = os.path.join("..", json_output["message"]["spans"][0]["file_name"])
                replace_in_file(target_file, ", Default", "")
                replace_in_file(target_file, "Default, ", "")

if __name__ == "__main__":
    logger.info('Will generate models from the API specifications')

    # Set working directory to be the same directory as this script
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    # Ensure git has no uncommitted changes
    if run(['git', 'status', '--porcelain']) != '':
        raise Exception('There are uncommitted changes in the repository')

    logging.info('Downloading schema...')
    os.makedirs(OPENAPI_TEMP_GENERATION_FOLDER, exist_ok=True)
    urllib.request.urlretrieve(API_SCHEMA_URL, os.path.join(OPENAPI_TEMP_GENERATION_FOLDER, 'gateway-api-schema.yaml'))

    logging.info('Preparing schema...')

    api_schema_temp_filename = os.path.join(OPENAPI_TEMP_GENERATION_FOLDER, 'gateway-api-schema.yaml')
    prepare_schema_for_generation(API_SCHEMA_LOCATION, api_schema_temp_filename)

    logging.info('Generating code from prepared schema...')

    generate_models(api_schema_temp_filename, os.path.join(OPENAPI_TEMP_GENERATION_FOLDER, "generated"), API_GENERATED_DESTINATION)

    logging.info("Code has been created.")

    safe_os_remove(OPENAPI_TEMP_GENERATION_FOLDER, silent=True)

    logging.info("Temp directory cleared.")

    logging.info("Running clippy...")

    run(['cargo', 'clippy', '--fix', '--allow-dirty'])
