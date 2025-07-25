from dbt_pipeline_utils.scripts.helpers.data_processors import DatabaseBC
from dbt_pipeline_utils.scripts.helpers.general import *

import synapseclient

class SynapseFileProcessor(DatabaseBC):
    def __init__(self, study_config, ftd_config, table_name, table_info, paths):
        super().__init__(study_config, ftd_config, table_name, table_info, paths)

        self.profile = 'synapse'
        self.src_schema = f'{self.study_id}_src_data'
        self.identifier = self.table_info.get("identifier")
        self.src_data_csv =  self.table_info.get("src_file_id") # Identifies the csv file containing data to be imported.
        

    def login_to_synapse():
        # If not logged in, log in to synapse
        try:
            syn = synapseclient.Synapse()
            syn.getUserProfile()
            logger.debug(f"Already logged in to Synapse.")
        except synapseclient.SynapseHTTPError as e:
            if e.response.status_code == 403: 
                logger.debug(f"Not logged in. Logging in to Synapse.")
                syn = synapseclient.Synapse()
                syn = synapseclient.login()
                logger.debug(f"Logged in to Synapse.")
            else:
                logger.error(f"Error checking Synapse login: {e}")
        return syn

    def get_syn_file(self):
        """
        Get a file from synapse, and store it in the src model's data dir

        Using the synapseclient, an excel file and metadata are cached. Refer to the
        path of the cached file to 'read it in'. Then, write the file to the dir it 
        is expected to be found by dbt_pipeline_utils functions. 
        """
        # pass
        logger.info(f"Logging in to Synapse.")
        syn = synapseclient.Synapse()
        syn = synapseclient.login()
        logger.info(f"Logged in to synapse")

        file_info = syn.get(self.identifier) # synapse id
        metadata_file_path = file_info.path

        data = read_file(metadata_file_path)

        output_path = self.paths['src_data_dir'] / Path(self.src_data_csv)

        if output_path.is_file():
            logger.debug(f"File already exists, not overwriting data.")
            return
        
        write_file(output_path, data)
    