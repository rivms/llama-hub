"""Azure Storage Blob file and directory reader.

A loader that fetches a file or iterates through a directory from Azure Storage Blob.

"""
import logging
import time
import math


import json
import re

from typing import Any, List, Optional, Union, Dict

from llama_index import download_loader
from llama_index.readers.base import BaseReader
from llama_index.readers.schema.base import Document

logger = logging.getLogger(__name__)

class AzDataExplorerReader(BaseReader):
    """General reader for any Azure Storage Blob file or directory.

    Args:
        service_name
    
    """

    def __init__(
        self,
        *args: Any,
        kcsb: Any,
        database: Optional[str], 
        query: str,
        **kwargs: Any
    ) -> None:
        """Initialize parameters for query"""
        super().__init__(*args, **kwargs)

        self.kcsb = kcsb
        self.database = database
        self.query = query

    def load_data(
            self,
            extra_info: Optional[Dict] = None) -> List[Document]:
        """Run query"""

        from azure.kusto.data import KustoClient

        kusto_client = KustoClient(self.kcsb)
       
        response = kusto_client.execute(self.database, self.query)

        result_table = response.primary_results[0]

        for row in result_table:
            json_output = json.dumps(row.to_dict(), indent=0, default=str)
            lines = json_output.split("\n")
            useful_lines = [
                line for line in lines if not re.match(r"^[{}\[\],]*$", line)
            ]
            return [Document("\n".join(useful_lines), extra_info=extra_info)]
