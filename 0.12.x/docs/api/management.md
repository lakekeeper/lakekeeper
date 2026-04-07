---
hide:
  - toc
---
<style>
/*
 * Remove redundant title that exists in Redocly to gain space
 */
.md-content__inner.md-typeset > h1,
.md-content__inner.md-typeset::before {
  display: none;
}


/*
 * Remove margins around the center body to gain space
 */
.md-content__inner.md-typeset > p,
.md-content__inner.md-typeset {
  margin: 0; 
  padding: 0;
}

/*
 * Remove upper margin (space) and width limitation (centering) on the main body
 */
.md-main__inner.md-grid {
  margin-top:0;
  max-width: none;
}

/*
 * Minimize the navigation sidebar to gain space but maintain navigation
 */
.md-sidebar.md-sidebar--primary {
  top: 0;
  width: 8rem;
}
</style>
<redoc spec-url="../management-open-api.yaml"></redoc>
<script src="https://cdn.redoc.ly/redoc/latest/bundles/redoc.standalone.js"> </script>
