data "kubectl_file_documents" "namespace" {
  content = file("../charts/argocd/namespace.yaml")
}
resource "kubectl_manifest" "namespace" {
  count              = length(data.kubectl_file_documents.namespace.documents)
  yaml_body          = element(data.kubectl_file_documents.namespace.documents, count.index)
  override_namespace = "argocd"
  depends_on = [
    data.kubectl_file_documents.namespace,
    module.eks
  ]
}

data "kubectl_file_documents" "argocd" {
  content = file("../charts/argocd/install.yaml")
}

resource "kubectl_manifest" "argocd" {
  count              = length(data.kubectl_file_documents.argocd.documents)
  yaml_body          = element(data.kubectl_file_documents.argocd.documents, count.index)
  override_namespace = "argocd"
  depends_on = [
    kubectl_manifest.namespace,
    data.kubectl_file_documents.argocd,
    module.eks
  ]
}

data "kubectl_file_documents" "git" {
  content = file("../charts/argocd/auth.yaml")
}

resource "kubectl_manifest" "git" {
  count              = length(data.kubectl_file_documents.git.documents)
  yaml_body          = element(data.kubectl_file_documents.git.documents, count.index)
  override_namespace = "argocd"
  depends_on = [
    kubectl_manifest.argocd,
    data.kubectl_file_documents.git
  ]
}

data "kubectl_file_documents" "airflow_key" {
  content = file("../airflow_access_git_repo/ssh.yaml")
}

resource "kubectl_manifest" "airflow_manifest" {
  count              = length(data.kubectl_file_documents.airflow_key.documents)
  yaml_body          = element(data.kubectl_file_documents.airflow_key.documents, count.index)
  override_namespace = "airflow"
  depends_on = [
    kubectl_manifest.argocd,
    data.kubectl_file_documents.airflow_key
  ]
}

data "kubectl_file_documents" "airflow" {
  content = file("../apps/airflow-app.yaml")
}

resource "kubectl_manifest" "airflow" {
  count              = length(data.kubectl_file_documents.airflow.documents)
  yaml_body          = element(data.kubectl_file_documents.airflow.documents, count.index)
  override_namespace = "argocd"
  depends_on = [
    kubectl_manifest.argocd,
    data.kubectl_file_documents.airflow,
    module.eks
  ]
}

data "kubectl_file_documents" "keys" {
  content = file("../secrets/keys.yml")
}

resource "kubectl_manifest" "keys" {
  count              = length(data.kubectl_file_documents.keys.documents)
  yaml_body          = element(data.kubectl_file_documents.keys.documents, count.index)
  override_namespace = "airflow"
  depends_on = [
    data.kubectl_file_documents.keys,
    data.kubectl_file_documents.airflow,
    kubectl_manifest.argocd,
    kubectl_manifest.airflow
  ]
}