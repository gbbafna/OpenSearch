/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node.remotestore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.cluster.coordination.JoinTaskExecutor;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Setting;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Contains all the method needed for a remote store backed node lifecycle.
 */
public class RemoteStoreNodeService {

    private static final Logger logger = LogManager.getLogger(RemoteStoreNodeService.class);
    private final Supplier<RepositoriesService> repositoriesService;
    private final ThreadPool threadPool;
    public static final Setting<CompatibilityMode> REMOTE_STORE_COMPATIBILITY_MODE_SETTING = new Setting<>(
        "remote_store.compatibility_mode",
        CompatibilityMode.STRICT.name(),
        CompatibilityMode::parseString,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Node join compatibility mode introduced with remote backed storage.
     *
     * @opensearch.internal
     */
    public enum CompatibilityMode {
        STRICT("strict");

        public final String mode;

        CompatibilityMode(String mode) {
            this.mode = mode;
        }

        public static CompatibilityMode parseString(String compatibilityMode) {
            try {
                return CompatibilityMode.valueOf(compatibilityMode.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "["
                        + compatibilityMode
                        + "] compatibility mode is not supported. "
                        + "supported modes are ["
                        + CompatibilityMode.values().toString()
                        + "]"
                );
            }
        }
    }

    public RemoteStoreNodeService(Supplier<RepositoriesService> repositoriesService, ThreadPool threadPool) {
        this.repositoriesService = repositoriesService;
        this.threadPool = threadPool;
    }

    /**
     * Creates a repository during a node startup and performs verification by invoking verify method against
     * mentioned repository. This verification will happen on a local node to validate if the node is able to connect
     * to the repository with appropriate permissions.
     * If the creation or verification fails this will close all the repositories this method created and throw
     * exception.
     */
    public List<Repository> createAndVerifyRepositories(DiscoveryNode localNode) {
        RemoteStoreNodeAttribute nodeAttribute = new RemoteStoreNodeAttribute(localNode);
        RepositoriesService reposService = repositoriesService.get();
        List<Repository> repositories = new ArrayList<>();
        for (RepositoryMetadata repositoryMetadata : nodeAttribute.getRepositoriesMetadata().repositories()) {
            String repositoryName = repositoryMetadata.name();
            Repository repository;
            RepositoriesService.validate(repositoryName);

            // Create Repository
            repository = reposService.createRepository(repositoryMetadata);
            logger.info(
                "remote backed storage repository with name [{}] and type [{}] created",
                repository.getMetadata().name(),
                repository.getMetadata().type()
            );

            // Verify Repository
            String verificationToken = repository.startVerification();
            repository.verify(verificationToken, localNode);
            repository.endVerification(verificationToken);
            logger.info(() -> new ParameterizedMessage("successfully verified [{}] repository", repositoryName));
            repositories.add(repository);
        }
        return repositories;
    }

    /**
     * Updates repositories metadata in the cluster state if not already present. If a repository metadata for a
     * repository is already present in the cluster state and if it's different then the joining remote store backed
     * node repository metadata an exception will be thrown and the node will not be allowed to join the cluster.
     */
    public RepositoriesMetadata updateRepositoriesMetadata(
        Optional<JoinTaskExecutor.Task> joiningTask,
        RepositoriesMetadata existingRepositories
    ) {
        if (joiningTask.isEmpty()) {
            return existingRepositories;
        }

        DiscoveryNode joiningNode = joiningTask.get().node();

        if (joiningNode.isRemoteStoreNode()) {
            List<RepositoryMetadata> updatedRepositoryMetadataList = new ArrayList<>();
            List<RepositoryMetadata> newRepositoryMetadataList = new RemoteStoreNodeAttribute(joiningNode).getRepositoriesMetadata()
                .repositories();

            if (existingRepositories == null) {
                return new RepositoriesMetadata(newRepositoryMetadataList);
            } else {
                updatedRepositoryMetadataList.addAll(existingRepositories.repositories());
            }

            for (RepositoryMetadata newRepositoryMetadata : newRepositoryMetadataList) {
                boolean repositoryAlreadyPresent = false;
                for (RepositoryMetadata existingRepositoryMetadata : existingRepositories.repositories()) {
                    if (newRepositoryMetadata.name().equals(existingRepositoryMetadata.name())) {
                        if (newRepositoryMetadata.equalsIgnoreGenerations(existingRepositoryMetadata)) {
                            repositoryAlreadyPresent = true;
                            break;
                        } else {
                            throw new IllegalStateException(
                                "new repository metadata ["
                                    + newRepositoryMetadata
                                    + "] supplied by joining node is different from existing repository metadata ["
                                    + existingRepositoryMetadata
                                    + "]."
                            );
                        }
                    }
                }
                if (repositoryAlreadyPresent == false) {
                    updatedRepositoryMetadataList.add(newRepositoryMetadata);
                }
            }
            return new RepositoriesMetadata(updatedRepositoryMetadataList);
        } else {
            return existingRepositories;
        }
    }
}
