<template>
  <v-container>
    <v-row>
      <v-col cols="6">
        <v-treeview
          :items="treeItems.items"
          item-key="id"
          item-title="title"
          :load-children="fetchUsers"
          select-strategy="independent"
          @click:select="updateSelected"
          v-model:selected="selected"
          open-strategy="list"
          return-object
        >
          <template #prepend="{ item }">
            <v-icon v-if="item.itemType == 'project'"> mdi-bookmark</v-icon>
            <v-icon v-else-if="item.itemType == 'warehouse'">
              mdi-database
            </v-icon
            >
            <v-icon v-else-if="item.itemType == 'namespace'">
              mdi-folder
            </v-icon
            >
            <v-icon v-else-if="item.itemType == 'table'"> mdi-table</v-icon>
            <v-icon v-else-if="item.itemType == 'view'">
              mdi-view-grid-outline
            </v-icon
            >
            <v-icon v-else>mdi-table</v-icon>
          </template>
        </v-treeview>
      </v-col>
      <v-col cols="6">
        <v-card>
          <v-card-title>Details {{ type }}: {{ obejctName }}</v-card-title>
          <v-card-text>
            <pre class="json-pre">{{ json }}</pre>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>
  </v-container>
</template>

<script lang="ts" setup>
import * as env from "@/app.config";
import {ref, onMounted} from "vue";
import {
  Data,
  Namespaces,
  Tables,
  TreeItem,
  TreeItems,
  Warehouse,
} from "../common/interfaces";

const treeItems = ref<TreeItems>({items: []});

const selected = ref([]);
const json = reactive({});
const baseUrl = env.default.icebergCatalogUrl as string;
const managementUrl = baseUrl + "/management/v1";
const catalogUrl = baseUrl + "/catalog/v1";
const type = ref();
const obejctName = ref();

onMounted(async () => {
  try {
    const data = (await loadData(managementUrl + "/project-list")) as Data;
    if (data) {
      const projects = data.projects;
      const transformedData: TreeItem[] = [];

      for (const project of projects) {
        transformedData.push({
          id: project.project_id,
          itemType: "project",
          title: `Project ${project.project_id}`,
          children: [], //children,
        });
      }

      treeItems.value = {items: transformedData};
    } else {
      console.error("Projects data is not available.");
    }
  } catch (err) {
    console.error("Failed to load data:", err);
  }
});

async function loadData(
  subPath: string
): Promise<Data | { warehouses: Warehouse[] } | Namespaces | Tables> {
  const res = await fetch(subPath);
  if (!res.ok) {
    throw new Error(`HTTP error! status: ${res.status}`);
  }
  return await res.json();
}

async function fetchUsers(item: any) {
  if (item.itemType == "project") {
    const warehousesResponse = (await loadData(
      managementUrl + "/warehouse?project-id=" + item.id
    )) as { warehouses: Warehouse[] };

    const children: TreeItem[] = warehousesResponse.warehouses.map(
      (warehouse) => ({
        id: warehouse.id,
        proejctId: item.id,
        itemType: "warehouse",
        title: warehouse.name,
        children: [],
      })
    );
    item.children.push(...children);

    return item;
  } else if (item.itemType == "warehouse") {
    const namespacesResponse = (await loadData(
      catalogUrl + "/" + item.id + "/namespaces"
    )) as Namespaces;

    const children: TreeItem[] = namespacesResponse.namespaces.flatMap(
      (namespaceArray) =>
        namespaceArray.map((namespace) => ({
          id: namespace,
          projectId: item.proejctId,
          whId: item.id,
          itemType: "namespace",
          title: namespace,
          children: [],
        }))
    );
    item.children.push(...children);

    return item;
  } else if (item.itemType == "namespace") {
    const tablesResponse = (await loadData(
      catalogUrl + "/" + item.whId + "/namespaces/" + item.id + "/tables"
    )) as Tables;

    const children: TreeItem[] = tablesResponse.identifiers.flatMap(
      (identifier) =>
        identifier.namespace.map((namespace) => ({
          id: `${identifier.name}`,
          projectId: item.proejctId,
          whId: item.whId,
          nsId: item.id,
          itemType: "table",
          title: `${identifier.name}`,
          children: [],
        }))
    );
    item.children.push(...children);

    const viewsResponse = (await loadData(
      catalogUrl + "/" + item.whId + "/namespaces/" + item.id + "/views"
    )) as Tables;

    const children_v: TreeItem[] = viewsResponse.identifiers.flatMap(
      (identifier) =>
        identifier.namespace.map((namespace) => ({
          id: `${identifier.name}`,
          projectId: item.proejctId,
          whId: item.whId,
          nsId: item.id,
          itemType: "view",
          title: `${identifier.name}`,
          children: [],
        }))
    );
    item.children.push(...children_v);

    return item;
  } else if (item.itemType == "table" || item.itemType == "view") {
    const res = await fetch(
      `${catalogUrl}/${item.whId}/namespaces/${item.nsId}/${item.itemType}s/${item.id}`
    );
    if (!res.ok) {
      throw new Error(`HTTP error! status: ${res.status}`);
    }

    type.value = item.itemType;
    obejctName.value = item.id;
    Object.assign(json, await res.json());
  }
}

function updateSelected(selectedItems: any) {
  console.log(selectedItems);
  console.log(treeItems.value);
  console.log(selected.value);
}
</script>

<style>
.json-pre {
  max-height: 80vh;
  overflow: auto;
}
</style>
