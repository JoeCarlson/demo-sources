package org.intermine.bio.dataconversion;

/*
 * Copyright (C) 2002-2016 FlyMine
 *
 * This code may be freely distributed and modified under the
 * terms of the GNU Lesser General Public Licence.  This should
 * be distributed with the code.  See the LICENSE file for more
 * information or http://www.gnu.org/copyleft/lesser.html.
 *
 */

import org.intermine.objectstore.proxy.ProxyReference;
import org.intermine.objectstore.query.*;

import org.intermine.model.bio.Organism;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.task.DBDirectDataLoaderTask;
import org.apache.tools.ant.BuildException;

import org.apache.log4j.Logger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;

/**
 * A demonstration of reading from external database and doing a straight
 * ETL operation into a production mine. This saves the integration step
 * using the items database, increasing the speed of loading in cases where
 * minimal merging of object is needed.
 *
 * @author Joe Carlson
 */
public class DbDirectDemoConverter  extends DBDirectDataLoaderTask
{
    // 
    private static final String DATASET_TITLE = "CHADO organisms";
    private static final String DATA_SOURCE_NAME = "CHADO DB";
    private static final String TAXONOMY_DB_NAME = "Taxonomy";
    private static final Logger LOG =
            Logger.getLogger(DbDirectDemoConverter.class);

    /**
     * {@inheritDoc}
     */
    public void process() {
        // a database has been initialised from properties starting with db.db-direct-demo

        Connection conn = getConnection();
        HashMap<String,ProxyReference> registeredOrganisms = queryForOrganisms();
        LOG.info("Retrieved " + registeredOrganisms.size() + " from the mine.");

        try {

            /*
             * In this rather contrived example, we're going to read the organism table
             * from a Chado databases and insert these into the mine in the organism table
             * The taxon id is stored in chado as a dbxref to a database named TAXONOMY_DB_NAME
             */

            /*
             * In this contrived example, we're going to first query the mine to see what is
             * already registered. There's no point on doing this for this example, but in
             * some cases you may want to use the data in the mine in the load processing.
             */
            Statement stmt = conn.createStatement();
            String orgQuery = "SELECT DISTINCT " +
                    "genus,species,abbreviation,common_name,accession as taxon_id FROM " +
                    "organism o, organism_dbxref od, dbxref d, db " +
                    "WHERE " +
                    "d.db_id=db.db_id " +
                    "AND db.name='" + TAXONOMY_DB_NAME + "' " +
                    "AND d.dbxref_id = od.dbxref_id " +
                    "AND o.organism_id=od.organism_id";
            int ctr = 0;
            ResultSet orgRes = stmt.executeQuery(orgQuery);
            while (orgRes.next()) {
                String genus = orgRes.getString("genus");
                String species = orgRes.getString("species");
                String taxonId = orgRes.getString("taxon_id");
                String shortName = orgRes.getString("abbreviation");
                String commonName = orgRes.getString("common_name");
                String organismName = genus + ' ' + species;
                if (registeredOrganisms.containsKey(organismName)) {
                    LOG.info(organismName + "has already been registered. Skipping...");
                } else {
                    Organism organismObject;
                    try {
                        organismObject = getDirectDataLoader().createObject(Organism.class);
                    } catch (ObjectStoreException e1) {
                        throw new BuildException("Problem getting an objectstore.");
                    }
                    if (genus != null) organismObject.setGenus(genus);
                    if (species != null) organismObject.setSpecies(species);
                    if (genus != null && species != null) organismObject.setName(organismName);
                    if (shortName != null) organismObject.setShortName(shortName);
                    if (taxonId != null) organismObject.setTaxonId(taxonId);
                    if (commonName != null) organismObject.setCommonName(commonName);

                    try {
                        getDirectDataLoader().store(organismObject);
                        ctr++;
                    } catch (ObjectStoreException e) {
                        throw new BuildException("Trouble storing organism: " + e.getMessage());
                    }
                }
            }
            orgRes.close();
            LOG.info("Registered " + ctr + " organisms.");
        } catch (SQLException e) {
            throw new BuildException("SQL Exception caught when processing: "+e.getMessage());
        }
    }

    private HashMap<String,ProxyReference> queryForOrganisms() {
        Query q = new Query();
        QueryClass qC = new QueryClass(Organism.class);
        q.addFrom(qC);
        QueryField qFId = new QueryField(qC,"id");
        q.addToSelect(qFId);
        QueryField qFName = new QueryField(qC,"name");
        q.addToSelect(qFName);

        HashMap<String,ProxyReference> orgMap = new HashMap<String,ProxyReference>();

        LOG.info("Prefilling Organism hash. Query is "+q);
        try {
            Results res = getIntegrationWriter().getObjectStore().execute(q,100000,false,false,false);
            Iterator<Object> resIter = res.iterator();
            LOG.info("Iterating...");
            while (resIter.hasNext()) {
                @SuppressWarnings("unchecked")
                ResultsRow<Object> rr = (ResultsRow<Object>) resIter.next();
                Integer orgId = (Integer)rr.get(0);
                String orgName = (String)rr.get(1);
                orgMap.put(orgName,new ProxyReference(getIntegrationWriter().getObjectStore(),orgId,Organism.class));
            }
        } catch (Exception e) {
            throw new BuildException("Problem in prefilling ProxyReferences Hash: " + e.getMessage());
        }

        return orgMap;

    }

    public String getDataSetTitle(String taxonId) {
        return DATASET_TITLE;
    }
}
