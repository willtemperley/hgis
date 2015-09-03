package io.hgis.gt;

import org.geotools.data.DataUtilities;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.memory.MemoryFeatureStore;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.Name;

import java.io.IOException;
import java.util.List;

/**
 *
 * Created by tempehu on 28-Apr-15.
 *
 */
public class HFeatureStore extends ContentDataStore {


    private final HFeatureSource hfs;

    public HFeatureStore(HFeatureSource hfs) {
        this.hfs = hfs;
    }

    @Override
    protected List<Name> createTypeNames() throws IOException {
        return null;
    }

    @Override
    protected ContentFeatureSource createFeatureSource(ContentEntry contentEntry) throws IOException {
        return hfs;
    }

}
