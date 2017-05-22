package com.google.enterprise.cloud.search.sharepoint;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.enterprise.adaptor.sharepoint.SiteDataClient;
import com.microsoft.schemas.sharepoint.soap.SiteDataSoap;
import com.microsoft.schemas.sharepoint.soap.directory.UserGroupSoap;
import com.microsoft.schemas.sharepoint.soap.people.PeopleSoap;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import javax.xml.namespace.QName;
import javax.xml.ws.BindingProvider;
import javax.xml.ws.EndpointReference;
import javax.xml.ws.Service;
import javax.xml.ws.wsaddressing.W3CEndpointReferenceBuilder;

class SiteConnectorFactoryImpl implements SiteConnectorFactory {
  private static final String XMLNS_DIRECTORY =
      "http://schemas.microsoft.com/sharepoint/soap/directory/";
  private static final String XMLNS = "http://schemas.microsoft.com/sharepoint/soap/";
  /** Map from Site or Web URL to SiteConnector object used to communicate with that Site/Web. */
  private final ConcurrentMap<String, SiteConnector> siteConnectors =
      new ConcurrentSkipListMap<String, SiteConnector>();
  private final SoapFactory soapFactory;
  private final SharePointRequestContext requestContext;
  private final boolean xmlValidation;

  private SiteConnectorFactoryImpl(Builder builder) {
    soapFactory = checkNotNull(builder.soapFactory);
    requestContext = checkNotNull(builder.requestContext);
    xmlValidation = builder.xmlValidation;
  }

  @Override
  public SiteConnector getInstance(String site, String web) throws IOException {
    web = getCanonicalUrl(web);
    SiteConnector siteConnector = siteConnectors.get(web);
    if (siteConnector != null) {
      return siteConnector;
    }
    site = getCanonicalUrl(site);
    String endpoint = getEndpoint(web + "/_vti_bin/SiteData.asmx");
    SiteDataSoap siteDataSoap = soapFactory.newSiteData(endpoint);
    String endpointUserGroup = getEndpoint(site + "/_vti_bin/UserGroup.asmx");
    UserGroupSoap userGroupSoap = soapFactory.newUserGroup(endpointUserGroup);
    String endpointPeople = getEndpoint(site + "/_vti_bin/People.asmx");
    PeopleSoap peopleSoap = soapFactory.newPeople(endpointPeople);
    requestContext.addContext((BindingProvider) siteDataSoap);
    requestContext.addContext((BindingProvider) userGroupSoap);
    requestContext.addContext((BindingProvider) peopleSoap);
    
    siteConnector =
        new SiteConnector.Builder(site, web)
            .setSiteDataClient(new SiteDataClient(siteDataSoap, xmlValidation))
            .setPeople(peopleSoap)
            .setUserGroup(userGroupSoap)
            .build();
    siteConnectors.putIfAbsent(web, siteConnector);
    siteConnector = siteConnectors.get(web);
    return siteConnector;
  }

  private static String getEndpoint(String url) throws IOException {
    try {
      return SharePointUrl.escape(url).toString();
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  // Remove trailing slash from URLs as SharePoint doesn't like trailing slash
  // in SiteData.GetUrlSegments
  private static String getCanonicalUrl(String url) {
    if (!url.endsWith("/")) {
      return url;
    }
    return url.substring(0, url.length() - 1);
  }
  
  @VisibleForTesting
  static class SoapFactoryImpl implements SoapFactory {
    private final Service siteDataService;
    private final Service userGroupService;
    private final Service peopleService;

    public SoapFactoryImpl() {
      this.siteDataService = SiteDataClient.createSiteDataService();
      this.userGroupService =
          Service.create(
              UserGroupSoap.class.getResource("/UserGroup.wsdl"),
              new QName(XMLNS_DIRECTORY, "UserGroup"));
      this.peopleService =
          Service.create(PeopleSoap.class.getResource("/People.wsdl"), new QName(XMLNS, "People"));
    }

    private static String handleEncoding(String endpoint) {
      // Handle Unicode. Java does not properly encode the POST path.
      return URI.create(endpoint).toASCIIString();
    }

    @Override
    public SiteDataSoap newSiteData(String endpoint) {
      EndpointReference endpointRef =
          new W3CEndpointReferenceBuilder().address(handleEncoding(endpoint)).build();
      return siteDataService.getPort(endpointRef, SiteDataSoap.class);
    }

    @Override
    public UserGroupSoap newUserGroup(String endpoint) {
      EndpointReference endpointRef =
          new W3CEndpointReferenceBuilder().address(handleEncoding(endpoint)).build();
      return userGroupService.getPort(endpointRef, UserGroupSoap.class);
    }

    @Override
    public PeopleSoap newPeople(String endpoint) {
      EndpointReference endpointRef =
          new W3CEndpointReferenceBuilder().address(handleEncoding(endpoint)).build();
      return peopleService.getPort(endpointRef, PeopleSoap.class);
    }
  }

  public static class Builder {
    private SoapFactory soapFactory;
    private SharePointRequestContext requestContext;
    private boolean xmlValidation;

    public Builder() {
      soapFactory = new SoapFactoryImpl();
      xmlValidation = false;
    }

    public Builder setSoapFactory(SoapFactory soapFactory) {
      this.soapFactory = soapFactory;
      return this;
    }

    public Builder setRequestContext(SharePointRequestContext requestContext) {
      this.requestContext = requestContext;
      return this;
    }

    public Builder setXmlValidation(boolean xmlValidation) {
      this.xmlValidation = xmlValidation;
      return this;
    }

    public SiteConnectorFactoryImpl build() {
      return new SiteConnectorFactoryImpl(this);
    }
  }
}
