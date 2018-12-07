package org.intermine.bio.dataconversion;

import java.io.File;
import java.io.FileReader;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.apache.tools.ant.BuildException;
import org.intermine.model.bio.Organism;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.task.FileDirectDataLoaderTask;
import org.intermine.util.FormattedTextParser;

/**
 * FileDirectDataLoaderTask
 *
 * This is a simple example of direct data loading from a file. We use
 * a file of tab-separated values to fill the Organism table. The fields are:
 * <Genus> <Species> <(optional) Common Name> <Short Name> <Taxon Id> <Name>
 *
 * @author Joe Carlson
 */
public class FileDirectDemoConverter extends FileDirectDataLoaderTask
{
    //
    private static final String DATASET_TITLE = "Demonstration of Direct Loading";
    private static final String DATA_SOURCE_NAME = "NCBI Taxonomy";
    protected static final Logger LOG = Logger.getLogger(FileDirectDemoConverter.class);

    /**
     * 
     *
     * {@inheritDoc}
     */
    public void processFile(File theFile) {
        Iterator<?> tsvIter;
        try {
            FileReader reader = new FileReader(theFile);
            tsvIter = FormattedTextParser.parseTabDelimitedReader(reader);
        } catch (Exception e) {
            throw new BuildException("Cannot parse file " + theFile, e);
        }
        int ctr = 0;
        while (tsvIter.hasNext()) {
            ctr++;
            String[] fields = (String[]) tsvIter.next();
            try {
                if (!processData(fields)) return;
            } catch (ObjectStoreException e) {
                throw new BuildException("Error procesing data",e);
            }
        }
        LOG.info("Processed " + ctr + " lines.");
    }

    private boolean processData(String[] fields) throws ObjectStoreException {
      
        if (fields.length != 6) {
            throw new BuildException("Unexpected number of columns in file.");
        }

        Organism org = getDirectDataLoader().createObject(Organism.class);
        org.setGenus(fields[0]);
        org.setSpecies(fields[1]);
        if (fields[2].length() > 0)  org.setCommonName(fields[2]);
        org.setShortName(fields[3]);
        org.setTaxonId(fields[4]);
        org.setName(fields[5]);

        try {
            getDirectDataLoader().store(org);
        } catch (ObjectStoreException e) {
            throw new BuildException("Problem storing organism",e);
        }
        return true;
    }
}
