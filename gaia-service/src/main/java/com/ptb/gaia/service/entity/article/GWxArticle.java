package com.ptb.gaia.service.entity.article;

import java.util.List;

/**
 * Created by eric on 16/6/24.
 */
public class GWxArticle extends GWxArticleBasic {
    List<GWxArticleDynamic> dynamics;

    public List<GWxArticleDynamic> getDynamics() {
        return dynamics;
    }

    public void setDynamics(List<GWxArticleDynamic> dynamics) {
        this.dynamics = dynamics;
    }
}
