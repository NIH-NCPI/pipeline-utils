from dbt_pipeline_utils.scripts.helpers.general import *

class TgtDocGenClass():

    def copy_directory(self):
        """
        Recursively copies files and subdirectories from src_dir to dest_dir
        """
        src_dir=self.paths["tgt_static_data_dir"]
        dest_dir= self.paths["dbtp_tgt_dir"]
        for item in src_dir.rglob("*"):
            relative_path = item.relative_to(src_dir)
            target = dest_dir / relative_path

            if item.is_dir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                # Copy file contents manually
                data = read_file(item)
                write_file(target, data)

        print(f"Copied '{src_dir}' to '{dest_dir}'")
