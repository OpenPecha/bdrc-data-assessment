import os
import pytest
import tempfile
from unittest.mock import patch, MagicMock
from rdflib import Dataset
from src.data_assessment.parser import (
    load_trig_file,
    batch_upload_to_neo4j,
    process_single_trig_file,

)


@pytest.fixture
def sample_trig_content():
    """Create a sample TRIG file content"""
    return """
    @prefix ex: <http://example.org/> .
    @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
    
    ex:Graph1 {
        ex:Subject1 rdf:type ex:Type1 .
        ex:Subject1 ex:predicate1 ex:Object1 .
    }
    """


@pytest.fixture
def sample_trig_file(sample_trig_content):
    """Create a temporary TRIG file for testing"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.trig', delete=False) as temp_file:
        temp_file.write(sample_trig_content)
        temp_file.close()
        yield temp_file.name
    os.unlink(temp_file.name)


@pytest.mark.filterwarnings("ignore::DeprecationWarning:rdflib.plugins.parsers.trig")
def test_load_trig_file(sample_trig_file):
    """Test loading a TRIG file"""
    graph = load_trig_file(sample_trig_file)
    assert isinstance(graph, Dataset)
    assert len(list(graph.contexts())) > 0


def test_batch_upload_to_neo4j():
    """Test batch upload to Neo4j"""
    mock_tx = MagicMock()
    triples = [{'subj': 'http://example.org/Subject1',
                'pred': 'http://example.org/predicate1', 'obj': 'http://example.org/Object1'}]
    batch_upload_to_neo4j(mock_tx, 'TestGraph', triples)
    mock_tx.run.assert_called_once()


@pytest.mark.filterwarnings("ignore::DeprecationWarning:rdflib.plugins.parsers.trig")
def test_process_single_trig_file(sample_trig_file):
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as log_file:
        log_file.close()
        try:
            with patch('src.data_assessment.parser.upload_named_graph_to_neo4j'):
                result = process_single_trig_file(sample_trig_file, log_file.name)
            assert result is True
            with open(log_file.name, 'r') as f:
                assert os.path.basename(sample_trig_file) in f.read()
        finally:
            os.unlink(log_file.name)
