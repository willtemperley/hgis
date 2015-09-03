package io.hgis.gt;

import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.beans.FeatureDescriptor;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Created by tempehu on 28-Apr-15.
 */
public class HFeatureSource extends ContentFeatureSource {

    private final ReferencedEnvelope env;
    private final List<SimpleFeature> features;
    private final HFeatureReader featureReader;
    private final SimpleFeatureType ft;

    public HFeatureSource(ContentEntry entry, ReferencedEnvelope referencedEnvelope, List<SimpleFeature> features, SimpleFeatureType ft) {
        super(entry, Query.ALL);
        this.ft = ft;
        this.env = referencedEnvelope;
        this.features = features;
        this.featureReader = new HFeatureReader(features, ft);
    }

    public class HFeatureReader implements FeatureReader<SimpleFeatureType, SimpleFeature> {

        private final SimpleFeatureType sft;
        private final Iterator<SimpleFeature> iterator;

        public HFeatureReader(List<SimpleFeature> features, SimpleFeatureType simpleFeatureType) {
            this.sft = simpleFeatureType;
            this.iterator = features.iterator();
        }

        public SimpleFeatureType getFeatureType() {
            return sft;
        }

        public SimpleFeature next() throws IOException, IllegalArgumentException, NoSuchElementException {
            return iterator.next();
        }

        public boolean hasNext() throws IOException {
            return iterator.hasNext();
        }

        public void close() throws IOException {
            
        }
    }

    @Override
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
        return env;
    }

    @Override
    protected int getCountInternal(Query query) throws IOException {
        return features.size();
    }

    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query) throws IOException {
        return featureReader;
    }

    @Override
    protected SimpleFeatureType buildFeatureType() throws IOException {
        return ft;
    }
}
