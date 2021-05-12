from app.config import Config
import json
import osparc
import tempfile
from time import sleep


def run_simulation(model_url, json_config):
    temp_config_file = tempfile.NamedTemporaryFile(mode="w+")

    json.dump(json_config, temp_config_file)

    temp_config_file.seek(0)

    try:
        api_client = osparc.ApiClient(osparc.Configuration(
            host=Config.OSPARC_API_URL,
            username=Config.OSPARC_API_KEY,
            password=Config.OSPARC_API_SECRET
        ))

        # Upload the configuration file.

        files_api = osparc.FilesApi(api_client)

        try:
            config_file = files_api.upload_file(temp_config_file.name)
        except:
            raise Exception(
                "the simulation configuration file could not be uploaded")

        # Create the simulation.

        solvers_api = osparc.SolversApi(api_client)

        solver = solvers_api.get_solver_release(
            "simcore/services/comp/opencor", "1.0.3")

        job = solvers_api.create_job(
            solver.id,
            solver.version,
            osparc.JobInputs({
                "model_url": model_url,
                "config_file": config_file
            })
        )

        # Start the simulation job.

        status = solvers_api.start_job(solver.id, solver.version, job.id)

        if status.state != "PUBLISHED":
            raise Exception("the simulation job could not be submitted")

        # Wait for the simulation job to be complete (or to fail).

        while True:
            status = solvers_api.inspect_job(solver.id, solver.version, job.id)

            if status.progress == 100:
                break

            sleep(1)

        status = solvers_api.inspect_job(solver.id, solver.version, job.id)

        if status.state != "SUCCESS":
            raise Exception("the simulation failed")

        # Retrieve the simulation job outputs.

        try:
            outputs = solvers_api.get_job_outputs(
                solver.id, solver.version, job.id)
        except:
            raise Exception(
                "the simulation job outputs could not be retrieved")

        # Download the simulation results.

        try:
            results_filename = files_api.download_file(
                outputs.results["output_1"].id)
        except:
            raise Exception("the simulation results could not be retrieved")

        results_file = open(results_filename, "r")

        res = {
            "status": "ok",
            "results": json.load(results_file)
        }

        results_file.close()
    except Exception as e:
        res = {
            "status": "nok",
            "description": e.args[0] if len(e.args) > 0 else "unknown"
        }

    temp_config_file.close()

    return res
