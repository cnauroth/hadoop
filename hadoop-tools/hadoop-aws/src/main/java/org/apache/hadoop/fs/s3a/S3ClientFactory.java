/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3AUtils.*;

import java.io.IOException;
import java.net.URI;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;

import com.google.common.base.Preconditions;

import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.s3native.S3xLoginHelper;
import org.apache.hadoop.util.VersionInfo;

import org.slf4j.Logger;

/**
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
interface S3ClientFactory {

  AmazonS3 createS3Client(URI name, URI uri) throws IOException;

  static class DefaultS3ClientFactory extends Configured
      implements S3ClientFactory {

    private static final Logger LOG = S3AFileSystem.LOG;

    @Override
    public AmazonS3 createS3Client(URI name, URI uri) throws IOException {
      Configuration conf = getConf();
      AWSCredentialsProvider credentials =
          getAWSCredentialsProvider(conf, name, uri);
      ClientConfiguration awsConf = new ClientConfiguration();
      initConnectionSettings(conf, awsConf);
      initProxySupport(conf, awsConf);
      initUserAgent(conf, awsConf);
      return createAmazonS3Client(conf, credentials, awsConf);
    }

    /**
     * Create the standard credential provider, or load in one explicitly
     * identified in the configuration.
     * @param conf configuration
     * @param uri the file system URI
     * @param binding the S3 binding/bucket.
     * @return a credential provider
     * @throws IOException on any problem. Class construction issues may be
     * nested inside the IOE.
     */
    private AWSCredentialsProvider getAWSCredentialsProvider(Configuration conf,
        URI binding, URI uri) throws IOException {
      AWSCredentialsProvider credentials;

      String className = conf.getTrimmed(AWS_CREDENTIALS_PROVIDER);
      if (StringUtils.isEmpty(className)) {
        S3xLoginHelper.Login creds = getAWSAccessKeys(binding, conf);
        credentials = new AWSCredentialsProviderChain(
            new BasicAWSCredentialsProvider(
                creds.getUser(), creds.getPassword()),
            new InstanceProfileCredentialsProvider(),
            new EnvironmentVariableCredentialsProvider());

      } else {
        try {
          LOG.debug("Credential provider class is {}", className);
          Class<?> credClass = Class.forName(className);
          try {
            credentials =
                (AWSCredentialsProvider)credClass.getDeclaredConstructor(
                    URI.class, Configuration.class).newInstance(uri, conf);
          } catch (NoSuchMethodException | SecurityException e) {
            credentials =
                (AWSCredentialsProvider)credClass.getDeclaredConstructor()
                    .newInstance();
          }
        } catch (ClassNotFoundException e) {
          throw new IOException(className + " not found.", e);
        } catch (NoSuchMethodException | SecurityException e) {
          throw new IOException(String.format("%s constructor exception.  A "
              + "class specified in %s must provide an accessible constructor "
              + "accepting URI and Configuration, or an accessible default "
              + "constructor.", className, AWS_CREDENTIALS_PROVIDER), e);
        } catch (ReflectiveOperationException | IllegalArgumentException e) {
          throw new IOException(className + " instantiation exception.", e);
        }
        LOG.debug("Using {} for {}.", credentials, uri);
      }

      return credentials;
    }

    private static void initConnectionSettings(Configuration conf,
        ClientConfiguration awsConf) {
      awsConf.setMaxConnections(intOption(conf, MAXIMUM_CONNECTIONS,
          DEFAULT_MAXIMUM_CONNECTIONS, 1));
      boolean secureConnections = conf.getBoolean(SECURE_CONNECTIONS,
          DEFAULT_SECURE_CONNECTIONS);
      awsConf.setProtocol(secureConnections ?  Protocol.HTTPS : Protocol.HTTP);
      awsConf.setMaxErrorRetry(intOption(conf, MAX_ERROR_RETRIES,
          DEFAULT_MAX_ERROR_RETRIES, 0));
      awsConf.setConnectionTimeout(intOption(conf, ESTABLISH_TIMEOUT,
          DEFAULT_ESTABLISH_TIMEOUT, 0));
      awsConf.setSocketTimeout(intOption(conf, SOCKET_TIMEOUT,
          DEFAULT_SOCKET_TIMEOUT, 0));
      int sockSendBuffer = intOption(conf, SOCKET_SEND_BUFFER,
          DEFAULT_SOCKET_SEND_BUFFER, 2048);
      int sockRecvBuffer = intOption(conf, SOCKET_RECV_BUFFER,
          DEFAULT_SOCKET_RECV_BUFFER, 2048);
      awsConf.setSocketBufferSizeHints(sockSendBuffer, sockRecvBuffer);
      String signerOverride = conf.getTrimmed(SIGNING_ALGORITHM, "");
      if (!signerOverride.isEmpty()) {
        LOG.debug("Signer override = {}", signerOverride);
        awsConf.setSignerOverride(signerOverride);
      }
    }

    void initProxySupport(Configuration conf, ClientConfiguration awsConf)
        throws IllegalArgumentException {
      String proxyHost = conf.getTrimmed(PROXY_HOST, "");
      int proxyPort = conf.getInt(PROXY_PORT, -1);
      if (!proxyHost.isEmpty()) {
        awsConf.setProxyHost(proxyHost);
        if (proxyPort >= 0) {
          awsConf.setProxyPort(proxyPort);
        } else {
          if (conf.getBoolean(SECURE_CONNECTIONS, DEFAULT_SECURE_CONNECTIONS)) {
            LOG.warn("Proxy host set without port. Using HTTPS default 443");
            awsConf.setProxyPort(443);
          } else {
            LOG.warn("Proxy host set without port. Using HTTP default 80");
            awsConf.setProxyPort(80);
          }
        }
        String proxyUsername = conf.getTrimmed(PROXY_USERNAME);
        String proxyPassword = conf.getTrimmed(PROXY_PASSWORD);
        if ((proxyUsername == null) != (proxyPassword == null)) {
          String msg = "Proxy error: " + PROXY_USERNAME + " or " +
              PROXY_PASSWORD + " set without the other.";
          LOG.error(msg);
          throw new IllegalArgumentException(msg);
        }
        awsConf.setProxyUsername(proxyUsername);
        awsConf.setProxyPassword(proxyPassword);
        awsConf.setProxyDomain(conf.getTrimmed(PROXY_DOMAIN));
        awsConf.setProxyWorkstation(conf.getTrimmed(PROXY_WORKSTATION));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Using proxy server {}:{} as user {} with password {} on " +
                  "domain {} as workstation {}", awsConf.getProxyHost(),
              awsConf.getProxyPort(),
              String.valueOf(awsConf.getProxyUsername()),
              awsConf.getProxyPassword(), awsConf.getProxyDomain(),
              awsConf.getProxyWorkstation());
        }
      } else if (proxyPort >= 0) {
        String msg =
            "Proxy error: " + PROXY_PORT + " set without " + PROXY_HOST;
        LOG.error(msg);
        throw new IllegalArgumentException(msg);
      }
    }

    /**
     * Initializes the User-Agent header to send in HTTP requests to the S3
     * back-end.  We always include the Hadoop version number.  The user also
     * may set an optional custom prefix to put in front of the Hadoop version
     * number.  The AWS SDK interally appends its own information, which seems
     * to include the AWS SDK version, OS and JVM version.
     *
     * @param conf Hadoop configuration
     * @param awsConf AWS SDK configuration
     */
    private void initUserAgent(Configuration conf,
        ClientConfiguration awsConf) {
      String userAgent = "Hadoop " + VersionInfo.getVersion();
      String userAgentPrefix = conf.getTrimmed(USER_AGENT_PREFIX, "");
      if (!userAgentPrefix.isEmpty()) {
        userAgent = userAgentPrefix + ", " + userAgent;
      }
      LOG.debug("Using User-Agent: {}", userAgent);
      awsConf.setUserAgent(userAgent);
    }

    private AmazonS3 createAmazonS3Client(Configuration conf,
        AWSCredentialsProvider credentials, ClientConfiguration awsConf)
        throws IllegalArgumentException {
      AmazonS3 s3 = new AmazonS3Client(credentials, awsConf);
      String endPoint = conf.getTrimmed(ENDPOINT, "");
      if (!endPoint.isEmpty()) {
        try {
          s3.setEndpoint(endPoint);
        } catch (IllegalArgumentException e) {
          String msg = "Incorrect endpoint: "  + e.getMessage();
          LOG.error(msg);
          throw new IllegalArgumentException(msg, e);
        }
      }
      enablePathStyleAccessIfRequired(s3, conf);
      return s3;
    }

    private void enablePathStyleAccessIfRequired(AmazonS3 s3,
        Configuration conf) {
      final boolean pathStyleAccess = conf.getBoolean(PATH_STYLE_ACCESS, false);
      if (pathStyleAccess) {
        LOG.debug("Enabling path style access!");
        s3.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
      }
    }

    /**
     * Get a integer option >= the minimum allowed value.
     * @param conf configuration
     * @param key key to look up
     * @param defVal default value
     * @param min minimum value
     * @return the value
     * @throws IllegalArgumentException if the value is below the minimum
     */
    static int intOption(Configuration conf, String key, int defVal, int min) {
      int v = conf.getInt(key, defVal);
      Preconditions.checkArgument(v >= min,
          String.format("Value of %s: %d is below the minimum value %d",
              key, v, min));
      return v;
    }
  }
}
