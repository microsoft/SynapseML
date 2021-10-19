import React from 'react';

import Link from '@docusaurus/Link';
import styles from './styles.module.css';

function NotebookCard(props) {
  const {url} = props;

  return (
    <Link to={`/docs/${url.url_path}`} className={styles.notebookCard}>
      <strong>{url.name}</strong>
    </Link>
  );
}

export default NotebookCard;
