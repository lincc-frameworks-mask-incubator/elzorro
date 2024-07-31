from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from hipscat.pixel_math.hipscat_id import healpix_to_hipscat_id, HIPSCAT_ID_COLUMN


class Writer:
    """Write tiles to parquet files, one file per split index
    
    It makes one row group per healpix order.
    
    Parameters
    ----------
    path : str or Path
        Directory where to write the parquet files.
    col_name : str
        Column name for the values.
    col_type : pyarrow.DataType
        Column type for the values, e.g. pa.uint8() or pa.float64().
    """

    def __init__(self, path, col_name, col_type):
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)

        self.col_name = col_name
        self.col_type = col_type

    def _create_parquet_writer(self, split_index):
        path = self.path / f'split_pixel_index={split_index:d}.parquet'
        return pq.ParquetWriter(
            path,
            pa.schema([
                pa.field(HIPSCAT_ID_COLUMN, pa.uint64()),
                pa.field('pixel_Norder', pa.uint8()),
                pa.field('pixel_Npix', pa.uint64()),
                pa.field(self.col_name, self.col_type),
            ]),
            write_statistics=False,
        )

    def _write_row_group(self, writer, norder, indexes, values):
        hipscat_index = healpix_to_hipscat_id(norder, indexes)
        table = pa.Table.from_arrays(
            [hipscat_index, np.full(hipscat_index.shape, norder, dtype=np.uint8), indexes, values],
            names=[HIPSCAT_ID_COLUMN, 'pixel_Norder', 'pixel_Npix', self.col_name]
        )
        writer.write_table(table)

    def write(self, split_index, tiles):
        """Write tiles to a parquet file for a given split index
        
        Parameters
        ----------
        split_index : int
            Split index.
        tiles : list of tuple
            List of tiles. Each tile is a tuple with:
            - norder: int, healpix order of the tile.
            - indexes: np.array, healpix indexes of the tile.
            - values: np.array, values for the tile.
        """
        with self._create_parquet_writer(split_index) as writer:
            for norder, indexes, values in tiles:
                self._write_row_group(writer, norder, indexes, values)

