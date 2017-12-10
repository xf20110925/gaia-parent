package com.ptb.gaia.service.entity.media;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by eric on 16/6/24.
 */
public class GMediaSet<T extends GMediaBasic> {
    static public GMediaSet NULL = new GMediaSet(0, new ArrayList<>());
    int total;
    List<T> medias;

    public GMediaSet() {
    }

    public GMediaSet(int i, List<T> objects) {
        this.total = i;
        this.medias = objects;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public List<T> getMedias() {
        return medias;
    }

    public void setMedias(List<T> medias) {
        this.medias = medias;
    }
}
