import { node } from 'prop-types';

export const apiUrl = 'http://localhost:8080/api';
export const baseUrl = 'http://localhost:8080';

export const uriClusters = id => {
  return `${apiUrl}/clusters${id ? '?clusterId=' + id : ''}`;
};

export const uriConnects = id => {
  return `${apiUrl}/connects${id ? '?clusterId=' + id : ''}`;
};

export const uriNodes = id => {
  return `${apiUrl}/cluster/nodes${id ? '?clusterId=' + id : ''}`;
};

export const uriTopics = (id, view, search) => {
  return `${apiUrl}/${
    search
      ? 'topicsByName?clusterId=' + id + '?view=' + view + '?search=' + search
      : 'topicsByType?clusterId=' + id + '?view=' + view
  } `;
};
export const uriNodesConfigs = (clusterId, nodeId) => {
  return (
    `${apiUrl}/cluster/nodes/configs${clusterId ? '?clusterId=' + clusterId : ''}` +
    `${nodeId ? '&nodeId=' + nodeId : ''}`
  );
};

export default { apiUrl, uriClusters, uriConnects, uriNodes, uriNodesConfigs };