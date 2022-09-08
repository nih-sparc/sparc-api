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

    solver_name = data["solver_name"]

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

        # Upload the configuration file, in the case of an OpenCOR simulation.

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

        # Create the simulation job with the job inputs that matches our
        # simulation type.

        solvers_api = osparc.SolversApi(api_client)

        try:
            solver = solvers_api.get_solver_release(
                solver_name, data["solver_version"])
        except ApiException as e:
            raise SimulationException(
                f"the requested solver could not be retrieved ({e})")

        if solver_name == OPENCOR_SOLVER:
            job_inputs = {
                "model_url": data["opencor"]["model_url"],
                "config_file": config_file
            }
        else:
            job_inputs = data["osparc"]["job_inputs"]

        job = solvers_api.create_job(
            solver.id,
            solver.version,
            osparc.JobInputs(job_inputs)
        )

        # Start the simulation job.

        status = solvers_api.start_job(solver.id, solver.version, job.id)

        if status.state != "PUBLISHED":
            raise SimulationException(
                "the simulation job could not be submitted")

        res = {
            "status": "ok",
            "data": {
                "solver_id": solver.id,
                "solver_version": solver.version,
                "job_id": job.id
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
        solver_id = data["solver_id"]
        solver_version = data["solver_version"]
        job_id = data["job_id"]
        status = solvers_api.inspect_job(solver_id, solver_version, job_id)

        if status.progress == 100:
            # The simulation has completed, but was it successful?

            if status.state != "SUCCESS":
                raise SimulationException("the simulation failed")

            # Retrieve the simulation job outputs.

            try:
                outputs = solvers_api.get_job_outputs(
                    solver_id, solver_version, job_id)
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

            if solver_id == OPENCOR_SOLVER:
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
