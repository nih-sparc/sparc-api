import json
import osparc
import tempfile

from app.config import Config
from flask import abort
from osparc.rest import ApiException
from time import sleep


OPENCOR_SOLVER = "simcore/services/comp/opencor"
DATASET_4_SOLVER = "simcore/services/comp/rabbit-ss-0d-cardiac-model"
DATASET_17_SOLVER = "simcore/services/comp/human-gb-0d-cardiac-model"
DATASET_78_SOLVER = "simcore/services/comp/kember-cardiac-model"


class SimulationException(Exception):
    pass


def start_simulation(data):
    # Determine the type of simulation.

    solver_name = data["solver"]["name"]

    if solver_name == OPENCOR_SOLVER:
        if not "opencor" in data:
            abort(400, description="Missing OpenCOR settings")
    else:
        if "osparc" in data:
            if ((solver_name != DATASET_4_SOLVER)
                and (solver_name != DATASET_17_SOLVER)
                    and (solver_name != DATASET_78_SOLVER)):
                abort(400, description="Unknown oSPARC solver")
        else:
            abort(400, description="Missing oSPARC settings")

    # Start the simulation.

    try:
        api_client = osparc.ApiClient(osparc.Configuration(
            host=Config.OSPARC_API_URL,
            username=Config.OSPARC_API_KEY,
            password=Config.OSPARC_API_SECRET
        ))

        # Upload the configuration file, in the case of an OpenCOR simulation or
        # in the case of an oSPARC simulation input file.

        has_solver_input = "input" in data["solver"]

        if solver_name == OPENCOR_SOLVER:
            temp_config_file = tempfile.NamedTemporaryFile(mode="w+")

            json.dump(data["opencor"]["json_config"], temp_config_file)

            temp_config_file.seek(0)

            try:
                files_api = osparc.FilesApi(api_client)

                config_file = files_api.upload_file(temp_config_file.name)
            except ApiException as e:
                raise SimulationException(
                    f"the simulation configuration file could not be uploaded ({e})")

            temp_config_file.close()
        elif has_solver_input:
            temp_input_file = tempfile.NamedTemporaryFile(mode="w+")

            temp_input_file.write(data["solver"]["input"]["value"])
            temp_input_file.seek(0)

            try:
                files_api = osparc.FilesApi(api_client)

                input_file = files_api.upload_file(temp_input_file.name)
            except ApiException as e:
                raise SimulationException(
                    f"the solver input file could not be uploaded ({e})")

            temp_input_file.close()

        # Create the simulation job with the job inputs that matches our
        # simulation type.

        solvers_api = osparc.SolversApi(api_client)

        try:
            solver = solvers_api.get_solver_release(
                solver_name, data["solver"]["version"])
        except ApiException as e:
            raise SimulationException(
                f"the requested solver could not be retrieved ({e})")

        if solver_name == OPENCOR_SOLVER:
            job_inputs = {
                "model_url": data["opencor"]["model_url"],
                "config_file": config_file
            }
        else:
            if has_solver_input:
                data["osparc"]["job_inputs"][data["solver"]["input"]["name"]] = input_file

            job_inputs = data["osparc"]["job_inputs"]

        job = solvers_api.create_job(
            solver.id,
            solver.version,
            osparc.JobInputs(job_inputs)
        )

        # Start the simulation job.

        status = solvers_api.start_job(solver.id, solver.version, job.id)

        if status.state not in {"PUBLISHED", "PENDING"}:
            raise SimulationException(
                "the simulation job could not be submitted")

        res = {
            "status": "ok",
            "data": {
                "job_id": job.id,
                "solver": {
                    "name": solver.id,
                    "version": solver.version
                }
            }
        }
    except SimulationException as e:
        res = {
            "status": "nok",
            "description": e.args[0] if len(e.args) > 0 else "unknown"
        }

    return res


def check_simulation(data):
    try:
        # Check whether the simulation has completed (or failed).

        api_client = osparc.ApiClient(osparc.Configuration(
            host=Config.OSPARC_API_URL,
            username=Config.OSPARC_API_KEY,
            password=Config.OSPARC_API_SECRET
        ))
        solvers_api = osparc.SolversApi(api_client)
        job_id = data["job_id"]
        solver_name = data["solver"]["name"]
        solver_version = data["solver"]["version"]
        status = solvers_api.inspect_job(solver_name, solver_version, job_id)

        if status.state == "FAILED":
            raise SimulationException("the simulation failed")

        if status.state == "SUCCESS":
            # Retrieve the simulation job outputs.

            try:
                outputs = solvers_api.get_job_outputs(
                    solver_name, solver_version, job_id)
            except ApiException as e:
                raise SimulationException(
                    f"the simulation job outputs could not be retrieved ({e})")

            # Download the simulation results.

            try:
                files_api = osparc.FilesApi(api_client)

                results_filename = files_api.download_file(
                    outputs.results[list(outputs.results.keys())[0]].id)
            except ApiException as e:
                raise SimulationException(
                    f"the simulation results could not be retrieved ({e})")

            results_file = open(results_filename, "r")

            res = {
                "status": "ok",
            }

            if solver_name == OPENCOR_SOLVER:
                res["results"] = json.load(results_file)
            else:
                res["results"] = results_file.read()

            results_file.close()
        else:
            # The simulation is not complete yet.

            res = {
                "status": "ok"
            }
    except SimulationException as e:
        res = {
            "status": "nok",
            "description": e.args[0] if len(e.args) > 0 else "unknown"
        }

    return res
