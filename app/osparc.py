import json
import osparc
import tempfile

from app.config import Config
from flask import abort
from osparc.rest import ApiException
from time import sleep


OPENCOR_SIMULATION = 0
DATASET_4_SIMULATION = 1
DATASET_17_SIMULATION = 2
DATASET_78_SIMULATION = 3


class SimulationException(Exception):
    pass


def do_run_simulation(data, simulation_type):
    try:
        api_client = osparc.ApiClient(osparc.Configuration(
            host=Config.OSPARC_API_URL,
            username=Config.OSPARC_API_KEY,
            password=Config.OSPARC_API_SECRET
        ))

        # Upload the configuration file, in the case of an OpenCOR simulation.

        files_api = osparc.FilesApi(api_client)

        if simulation_type == OPENCOR_SIMULATION:
            temp_config_file = tempfile.NamedTemporaryFile(mode="w+")

            json.dump(data["opencor"]["json_config"], temp_config_file)

            temp_config_file.seek(0)

            try:
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
                data["solver_name"], data["solver_version"])
        except ApiException as e:
            raise SimulationException(
                f"the requested solver could not be retrieved ({e})")

        if simulation_type == OPENCOR_SIMULATION:
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

        # Wait for the simulation job to complete (or to fail).

        while True:
            status = solvers_api.inspect_job(
                solver.id, solver.version, job.id)

            if status.progress == 100:
                break

            sleep(1)

        status = solvers_api.inspect_job(
            solver.id, solver.version, job.id)

        if status.state != "SUCCESS":
            raise SimulationException("the simulation failed")

        # Retrieve the simulation job outputs.

        try:
            outputs = solvers_api.get_job_outputs(
                solver.id, solver.version, job.id)
        except ApiException as e:
            raise SimulationException(
                f"the simulation job outputs could not be retrieved ({e})")

        # Download the simulation results.

        if simulation_type == OPENCOR_SIMULATION:
            output_name = "output_1"
        else:
            output_name = "out_1"

        try:
            results_filename = files_api.download_file(
                outputs.results[output_name].id)
        except ApiException as e:
            raise SimulationException(
                f"the simulation results could not be retrieved ({e})")

        results_file = open(results_filename, "r")

        res = {
            "status": "ok",
        }

        if simulation_type == OPENCOR_SIMULATION:
            res["results"] = json.load(results_file)
        else:
            res["results"] = results_file.read()

        results_file.close()
    except SimulationException as e:
        res = {
            "status": "nok",
            "description": e.args[0] if len(e.args) > 0 else "unknown"
        }

    return res


def run_simulation(data):
    if data["solver_name"] == "simcore/services/comp/opencor":
        if "opencor" in data:
            return do_run_simulation(data, OPENCOR_SIMULATION)
        else:
            abort(400, description="Missing OpenCOR settings")
    else:
        if "osparc" in data:
            if data["solver_name"] == "simcore/services/comp/rabbit-ss-0d-cardiac-model":
                return do_run_simulation(data, DATASET_4_SIMULATION)
            elif data["solver_name"] == "simcore/services/comp/human-gb-0d-cardiac-model":
                return do_run_simulation(data, DATASET_17_SIMULATION)
            elif data["solver_name"] == "simcore/services/comp/kember-cardiac-model":
                return do_run_simulation(data, DATASET_78_SIMULATION)
            else:
                abort(400, description="Unknown oSPARC solver")
        else:
            abort(400, description="Missing oSPARC settings")
