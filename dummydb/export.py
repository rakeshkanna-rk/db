import os
import zipfile
import tarfile
from typing import List, Union
from bson import decode_all

class ExportManager:
    """Export Manager for exporting database, cache, and archives."""

    def __init__(self, dbname: str):
        """
        Initialize the ExportManager.

        Args:
            dbname (str): Path to the directory containing database files.
            cache_dir (str): Path to the directory containing cache files.
        """
        self.dbname = dbname
        self.cache_dir = os.path.join(dbname, ".cache")

    def export_full_db(self, output_file: str, file_format: str = "zip") -> None:
        """
        Export the full database and cache into a single archive.

        Args:
            output_file (str): Path to the output file (e.g., "backup.zip").
            file_format (str): Archive format, either "zip" or "tar". Default is "zip".

        Raises:
            ValueError: If an unsupported file format is provided.
        """
        if file_format not in ["zip", "tar"]:
            raise ValueError("Unsupported file format. Use 'zip' or 'tar'.")

        with self._create_archive(output_file, file_format) as archive:
            self._add_directory_to_archive(self.dbname, archive, file_format)
            self._add_directory_to_archive(self.cache_dir, archive, file_format)

    def export_tables(self, output_file: str, file_format: str = "zip", *table_names: str) -> None:
        """
        Export specified database tables into a single archive.

        Args:
            output_file (str): Path to the output file (e.g., "tables_backup.zip").
            file_format (str): Archive format, either "zip" or "tar". Default is "zip".
            *table_names (str): Names of the database tables to export.

        Raises:
            FileNotFoundError: If any of the specified table files are not found.
            ValueError: If an unsupported file format is provided.
        """
        if file_format not in ["zip", "tar"]:
            raise ValueError("Unsupported file format. Use 'zip' or 'tar'.")

        with self._create_archive(output_file, file_format) as archive:
            for table_name in table_names:
                print(f"Exporting table '{table_name}'...")
                table_path = os.path.join(self.dbname, table_name)
                print(f"Table path: {table_path}")
                if not os.path.exists(table_path):
                    raise FileNotFoundError(f"Table '{table_name}' not found in {self.dbname}.")
                self._add_file_to_archive(table_path, archive, file_format)

    def export_cache(self, output_file: str, file_format: str = "zip") -> None:
        """
        Export the entire cache directory into an archive.

        Args:
            output_file (str): Path to the output file (e.g., "cache_backup.zip").
            file_format (str): Archive format, either "zip" or "tar". Default is "zip".

        Raises:
            ValueError: If an unsupported file format is provided.
        """
        if file_format not in ["zip", "tar"]:
            raise ValueError("Unsupported file format. Use 'zip' or 'tar'.")

        with self._create_archive(output_file, file_format) as archive:
            self._add_directory_to_archive(self.cache_dir, archive, file_format)

    def _create_archive(self, output_file: str, file_format: str) -> Union[zipfile.ZipFile, tarfile.TarFile]:
        """
        Create an archive for exporting data.

        Args:
            output_file (str): Path to the output file.
            file_format (str): Archive format, either "zip" or "tar".

        Returns:
            Union[zipfile.ZipFile, tarfile.TarFile]: Archive object.
        """
        if file_format == "zip":
            return zipfile.ZipFile(output_file, "w", zipfile.ZIP_DEFLATED)
        elif file_format == "tar":
            return tarfile.open(output_file, "w")

    def _add_directory_to_archive(self, directory: str, archive, file_format: str) -> None:
        """
        Add a directory to an archive.

        Args:
            directory (str): Path to the directory to add.
            archive (Union[zipfile.ZipFile, tarfile.TarFile]): Archive object.
            file_format (str): Archive format, either "zip" or "tar".
        """
        for root, _, files in os.walk(directory):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, start=directory)
                self._add_file_to_archive(file_path, archive, file_format, arcname)

    def _add_file_to_archive(self, file_path: str, archive, file_format: str, arcname: str = None) -> None:
        """
        Add a file to an archive.

        Args:
            file_path (str): Path to the file to add.
            archive (Union[zipfile.ZipFile, tarfile.TarFile]): Archive object.
            file_format (str): Archive format, either "zip" or "tar".
            arcname (str): Archive name for the file (optional).
        """
        if arcname is None:
            arcname = os.path.basename(file_path)

        if file_format == "zip":
            archive.write(file_path, arcname=arcname)
        elif file_format == "tar":
            archive.add(file_path, arcname=arcname)
