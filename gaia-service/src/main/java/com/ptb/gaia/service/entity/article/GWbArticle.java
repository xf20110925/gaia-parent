package com.ptb.gaia.service.entity.article;

import java.util.List;

/**
 * Created by eric on 16/6/24.
 */
public class GWbArticle extends GWbArticleBasic {
    List<GWbArticleDynamic> dynamics;

    public List<GWbArticleDynamic> getDynamics() {
        return dynamics;
    }

    public void setDynamics(List<GWbArticleDynamic> dynamics) {
        this.dynamics = dynamics;
    }
}
