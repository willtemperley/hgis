package io.hgis.gt;

import org.geotools.data.*;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;
import org.opengis.filter.Filter;

import java.awt.*;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Created by tempehu on 28-Apr-15.
 */
public class HFeatureSource2 implements SimpleFeatureSource {

    private final ReferencedEnvelope env;
    private final List<SimpleFeature> features;
    private final SimpleFeatureType ft;

    public HFeatureSource2(ContentEntry entry, ReferencedEnvelope referencedEnvelope, List<SimpleFeature> features, SimpleFeatureType ft) {
        this.ft = ft;
        this.env = referencedEnvelope;
        this.features = features;
    }

    public SimpleFeatureCollection getFeatures() throws IOException {
        return null;
    }

    public SimpleFeatureType getSchema() {
        return null;
    }

    public ReferencedEnvelope getBounds() throws IOException {
        return null;
    }

    public ReferencedEnvelope getBounds(Query query) throws IOException {
        return null;
    }

    public int getCount(Query query) throws IOException {
        return 0;
    }

    public Set<RenderingHints.Key> getSupportedHints() {
        return null;
    }

    public Name getName() {
        return null;
    }

    public ResourceInfo getInfo() {
        return null;
    }

    public DataAccess<SimpleFeatureType, SimpleFeature> getDataStore() {
        return null;
    }

    public QueryCapabilities getQueryCapabilities() {
        return new QueryCapabilities();
    }

    public void addFeatureListener(FeatureListener featureListener) {

    }

    public void removeFeatureListener(FeatureListener featureListener) {

    }

    public SimpleFeatureCollection getFeatures(Filter filter) throws IOException {
        return null;
    }

    public SimpleFeatureCollection getFeatures(Query query) throws IOException {
        return null;
    }

}
